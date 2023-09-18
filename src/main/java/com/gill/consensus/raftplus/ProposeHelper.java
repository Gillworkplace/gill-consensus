package com.gill.consensus.raftplus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.AppendLogReply;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.model.LogEntry;
import com.gill.consensus.raftplus.service.InnerNodeService;
import com.gill.consensus.raftplus.service.PrintService;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * ProposeHelper
 *
 * @author gill
 * @version 2023/09/11
 **/
@Slf4j
public class ProposeHelper implements PrintService {

	private final ConcurrentSkipListMap<Integer, WaitLogEntry> proposeQueue = new ConcurrentSkipListMap<>();

	private List<NodeProxy> followerProxies = Collections.emptyList();

	private final Supplier<ExecutorService> apiPoolSupplier;

	@Override
	public String println() {
		StringBuilder sb = new StringBuilder();
		sb.append("propose queue: ").append(proposeQueue).append(System.lineSeparator());
		sb.append("followerProxies: ").append(System.lineSeparator());
		for (NodeProxy proxy : followerProxies) {
			sb.append(proxy.println());
		}
		return sb.toString();
	}

	@Getter
	@ToString
	private static class WaitLogEntry {

		private final Thread thread;

		private final LogEntry logEntry;

		public WaitLogEntry(Thread thread, LogEntry logEntry) {
			this.thread = thread;
			this.logEntry = logEntry;
		}
	}

	public ProposeHelper(Supplier<ExecutorService> apiPoolSupplier) {
		this.apiPoolSupplier = apiPoolSupplier;
	}

	/**
	 * 启动
	 * 
	 * @param node
	 *            节点
	 * @param followers
	 *            从者
	 * @param preLogIdx
	 *            日志索引
	 */
	public void start(Node node, List<InnerNodeService> followers, int preLogIdx) {
		List<NodeProxy> proxies = followers.stream().map(follower -> new NodeProxy(node, follower, preLogIdx))
				.collect(Collectors.toList());
		proxies.forEach(NodeProxy::start);
		followerProxies = proxies;
	}

	/**
	 * 停止
	 */
	public void clear() {
		List<NodeProxy> proxies = followerProxies;
		followerProxies = Collections.emptyList();
		log.info("start to clear propose helper's proxies");
		CompletableFuture<?>[] futures = proxies.stream().map(proxy -> CompletableFuture.runAsync(proxy::stop))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(futures).join();
		log.info("finish clearing propose helper's proxies");
	}

	/**
	 * propose
	 * 
	 * @param logEntry
	 *            日志
	 * @return 是否成功
	 */
	public int propose(LogEntry logEntry, Runnable dataStorageApplier) {
		int logIdx = logEntry.getIndex();
		proposeQueue.put(logIdx, new WaitLogEntry(Thread.currentThread(), logEntry));
		boolean success = Utils.majorityCall(followerProxies, proxy -> {
			AppendLogReply reply = new AppendLogReply(false, -1);
			try {
				reply = proxy.appendLog(logEntry);
				return reply;
			} catch (Exception e) {
				log.error("call propose to {} failed, logEntry: {}, e: {}", proxy.getID(), logEntry, e.getMessage());
			}
			log.error("call propose to {} failed, logEntry: {}, reply: {}", proxy.getID(), logEntry, reply);
			return reply;
		}, Reply::isSuccess, apiPoolSupplier.get(), "propose");
		if (success) {
			int count = 0;
			while (proposeQueue.firstKey() != logIdx) {
				if (count++ >= 16) {
					LockSupport.park();
				}
			}
			dataStorageApplier.run();
		} else {
			log.warn("propose failed, logEntry: {}", logEntry);
		}
		proposeQueue.remove(logIdx);
		Map.Entry<Integer, WaitLogEntry> nextEntry = proposeQueue.firstEntry();
		if (nextEntry != null) {
			LockSupport.unpark(nextEntry.getValue().getThread());
		}
		return success ? logIdx : -1;
	}
}
