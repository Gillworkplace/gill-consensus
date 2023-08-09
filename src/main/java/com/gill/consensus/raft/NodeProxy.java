package com.gill.consensus.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gill.consensus.raft.model.LogEntries;
import com.gill.consensus.raft.model.LogEntry;
import com.gill.consensus.raft.model.LogEntryReply;
import com.gill.consensus.raft.model.Reply;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * NodeProxy
 *
 * @author gill
 * @version 2023/08/03
 **/
@Slf4j
@Getter
public class NodeProxy {

	private static final long TIMEOUT = 50L;

	private static final int BATCH = 100;

	private final Node self;

	private final Node node;

	private final ExecutorService executor;

	private final PriorityBlockingQueue<LogEntryReply> logs = new PriorityBlockingQueue<>(16,
			Comparator.comparingInt(l -> l.logEntry.index));

	@Override
	public String toString() {
		return "NodeProxy{" + "waitingSendLogs=" + logs + '}';
	}

	public NodeProxy(Node self, Node node) {
		this.self = self;
		this.node = node;
		this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10),
				r -> new Thread(r, "node-" + self.getId() + "-" + node.getId() + "-proxy"));
		this.executor.execute(() -> {

			// noinspection InfiniteLoopStatement
			while (true) {
				List<LogEntryReply> logEntryReplies = new ArrayList<>();
				List<LogEntry> toAppendLogs = new ArrayList<>();
				LogEntryReply entry;
				try {
					int cnt = 0;
					while (cnt++ < BATCH && (entry = logs.poll(TIMEOUT, TimeUnit.MILLISECONDS)) != null) {
						toAppendLogs.add(entry.logEntry);
						logEntryReplies.add(entry);
					}
				} catch (InterruptedException e) {
					log.error("node {}, poll logs interrupted, e: {}", node.getId(), e);
				}
				if (toAppendLogs.size() == 0) {
					continue;
				}
				LogEntry preNode = self.getLogs().get(toAppendLogs.get(0).index - 1);
				int term = self.getCurrentTerm().get();
				int committedIndex = self.getCommittedIndex().get();
				LogEntries logEntries = new LogEntries(self, term, toAppendLogs, preNode.index, preNode.term,
						committedIndex);
				log.trace("do propose {} to node {}", logEntries, node.getId());
				Reply reply = node.appendLogEntries(logEntries);
				if (!reply.success) {

					// 发现有term比本节点更大，降级成follower
					if (reply.term > term) {
						self.getState().downgrade();
					} else {

						// 尝试等待修复数据
						CountDownLatch latch = new CountDownLatch(1);

						// 异步修复follower数据
						log.debug("node-{}-{} sync preNode {}, node.term {}, committedIndex {}", self.getId(),
								node.getId(), preNode, term, committedIndex);
						CompletableFuture.runAsync(() -> {
							dfsSync(preNode, term, committedIndex);
							latch.countDown();
						});
						try {
							if (latch.await(500, TimeUnit.MILLISECONDS)) {

								// 重试append
								reply = node.appendLogEntries(logEntries);
							}
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}

					}
				}
				for (LogEntryReply logEntryReply : logEntryReplies) {
					logEntryReply.reply = new Reply(reply.term, reply.success);
					logEntryReply.latch.countDown();
				}
			}
		});
	}

	private void dfsSync(LogEntry tNode, int term, int committedIndex) {
		LogEntry preNode = self.getLogs().get(tNode.index - 1);

		// 尝试同步节点tNode
		LogEntries logs = new LogEntries(self, term, Collections.singletonList(tNode), preNode.index, preNode.term,
				committedIndex);
		Reply reply = node.appendLogEntries(logs);
		if (!reply.success) {
			if (reply.term > term) {
				self.getState().downgrade();
				return;
			} else {

				// 同步失败，说明上一个节点不一致，需要同步上一个节点
				dfsSync(preNode, term, committedIndex);
			}
		}

		// 上一个节点同步完成再次同步该节点
		node.appendLogEntries(logs);
	}

	/**
	 * propose
	 * 
	 * @param logEntry
	 *            logEntry
	 * @return boolean
	 */
	public Reply appendEntry(LogEntry logEntry) {
		CountDownLatch latch = new CountDownLatch(1);
		LogEntryReply logEntryReply = new LogEntryReply(logEntry, latch);
		try {
			logs.offer(logEntryReply);
			if (!latch.await(100, TimeUnit.MILLISECONDS)) {
				log.info("propose {} to node-{}-{} timeout", logEntry, self.getId(), node.getId());
			}
		} catch (InterruptedException e) {
			log.error(e.toString());
		}
		return logEntryReply.reply;
	}
}
