package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.gill.consensus.raftplus.apis.DataStorage;
import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.AppendLogEntriesParam;
import com.gill.consensus.raftplus.entity.AppendLogReply;
import com.gill.consensus.raftplus.entity.ReplicateSnapshotParam;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.exception.SyncSnapshotException;
import com.gill.consensus.raftplus.model.LogEntry;
import com.gill.consensus.raftplus.model.Snapshot;
import com.gill.consensus.raftplus.service.InnerNodeService;
import com.gill.consensus.raftplus.service.PrintService;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * NodeProxy
 *
 * @author gill
 * @version 2023/09/11
 **/
@SuppressWarnings("AlibabaServiceOrDaoClassShouldEndWithImpl")
@Slf4j
public class NodeProxy implements Runnable, PrintService {

	private static final long TIMEOUT = 50L;

	private static final int BATCH = 100;

	private final Node self;

	private final InnerNodeService follower;

	private int preLogIdx;

	private final ConcurrentSkipListMap<Integer, LogEntryReply> logs = new ConcurrentSkipListMap<>();

	private final ExecutorService executor;

	private volatile boolean running = true;

	public NodeProxy(Node self, InnerNodeService follower, int lastLogIdx) {
		this.self = self;
		this.follower = follower;
		this.preLogIdx = lastLogIdx;
		this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10),
				r -> new Thread(r, "node-" + self.getID() + "-" + follower.getID() + "-proxy"));
	}

	public int getID() {
		return follower.getID();
	}

	@Override
	public String println() {
		StringBuilder sb = new StringBuilder();
		sb.append("running: ").append(running).append(System.lineSeparator());
		sb.append("preLogIdx: ").append(preLogIdx).append(System.lineSeparator());
		sb.append("waiting append logs: ").append(logs).append(System.lineSeparator());
		return sb.toString();
	}

	/**
	 * 启动
	 */
	public void start() {
		running = true;
		this.executor.execute(this);
	}

	/**
	 * 停止
	 */
	public void stop() {
		running = false;
		this.executor.shutdownNow();
		Utils.awaitTermination(this.executor, "proxy-" + self.getID() + "-" + follower.getID());
	}

	/**
	 * 单线程运行，无并发问题
	 */
	@Override
	public void run() {
		while (running) {
			if (skip()) {
				continue;
			}
			List<LogEntryReply> entries = pollSuccessiveLogs();
			List<LogEntry> appendLogs = entries.stream().map(LogEntryReply::getLogEntry).collect(Collectors.toList());
			try {
				AppendLogReply reply = doAppendLogs(appendLogs);
				handleReply(entries, reply);
			} catch (Exception e) {
				log.error("node: {} appends logs to {} failed, e: {}", self.getID(), follower.getID(), e.getMessage());
				putbackLogs(entries);
			}
		}
	}

	private boolean skip() {
		if (logs.isEmpty() || logs.firstKey() != preLogIdx + 1) {
			try {
				Thread.sleep(TIMEOUT);
			} catch (InterruptedException ignored) {
			}
			return true;
		}
		return false;
	}

	private List<LogEntryReply> pollSuccessiveLogs() {
		List<LogEntryReply> entries = new ArrayList<>();
		for (int i = 0, preIdx = preLogIdx; i < BATCH && !logs.isEmpty()
				&& logs.firstKey() == preIdx + 1; i++, preIdx++) {
			entries.add(logs.pollFirstEntry().getValue());
		}
		return entries;
	}

	private AppendLogReply doAppendLogs(List<LogEntry> appendLogs) {
		int nodeId = self.getID();
		long term = self.getTerm();
		long preLogTerm = self.getLogManager().getLog(preLogIdx).getTerm();
		int committedIdx = self.getCommittedIdx();
		AppendLogEntriesParam param = AppendLogEntriesParam.builder(nodeId, term).preLogTerm(preLogTerm)
				.preLogIdx(preLogIdx).commitIdx(committedIdx).logs(appendLogs).build();
		log.debug("node: {} proposes to {}, logs: {}", nodeId, follower.getID(), appendLogs);
		return follower.appendLogEntries(param);
	}

	private void handleReply(List<LogEntryReply> entries, AppendLogReply reply) throws SyncSnapshotException {
		if (reply.isSuccess()) {
			handleSuccess(entries);
		} else if (reply.getTerm() > self.getTerm()) {

			// 服务端任期大于本机，则更新任期并降级为follower
			self.stepDown(reply.getTerm());
		} else if (reply.isSyncSnapshot()) {

			// 重新同步快照信息及日志
			syncSnapshot();
		} else {

			// 修复follower旧日志
			repairOldLogs(reply.getCompareIdx());
		}
	}

	private void handleSuccess(List<LogEntryReply> entries) {
		int lastLogIdx = lastLogIdx(entries);
		preLogIdx = lastLogIdx;
		self.setCommittedIdx(lastLogIdx);
		for (LogEntryReply entry : entries) {
			Optional.ofNullable(entry.getLatch()).ifPresent(CountDownLatch::countDown);
		}
	}

	private void syncSnapshot() throws SyncSnapshotException {
		int nodeId = self.getID();
		log.debug("node: {} sync snapshot to {}", nodeId, follower.getID());
		DataStorage dataStorage = self.getDataStorage();
		Snapshot snapshot = dataStorage.getSnapshot();
		long term = self.getTerm();
		long applyTerm = snapshot.getApplyTerm();
		int applyIdx = snapshot.getApplyIdx();
		byte[] data = snapshot.getData();
		ReplicateSnapshotParam param = new ReplicateSnapshotParam(nodeId, term, applyIdx, applyTerm, data);
		Reply reply = follower.replicateSnapshot(param);
		if (!reply.isSuccess()) {
			throw new SyncSnapshotException(
					String.format("node: %s sync snapshot to %s failed, term: %s", nodeId, follower.getID(), term));
		}
	}

	private void repairOldLogs(int compareIdx) throws SyncSnapshotException {
		if (compareIdx < 0) {
			return;
		}
		DataStorage dataStorage = self.getDataStorage();

		// 如果compareIdx小于 snapshot 的 applyIdx说明同步的日志可能已被删除，直接同步快照
		if (compareIdx < dataStorage.getApplyIdx()) {
			syncSnapshot();
			return;
		}
		log.debug("node: {} repair logs to {}, compare idx: {}", self.getID(), follower.getID(), compareIdx);

		// 从日志中获取compareIdx到队列第一个元素的所有日志
		LogManager logManager = self.getLogManager();
		int endIdx = Optional.ofNullable(logs.firstKey()).orElse(Integer.MAX_VALUE);
		List<LogEntry> entries = logManager.getLogs(compareIdx + 1, endIdx);
		for (LogEntry logEntry : entries) {
			int logIdx = logEntry.getIndex();
			LogEntryReply entry = new LogEntryReply(null, logEntry);
			logs.put(logIdx, entry);
		}
		preLogIdx = compareIdx;
	}

	private void putbackLogs(List<LogEntryReply> entries) {
		for (LogEntryReply entry : entries) {
			logs.put(entry.getLogEntry().getIndex(), entry);
		}
	}

	private static int lastLogIdx(List<LogEntryReply> appendLogs) {
		return appendLogs.get(appendLogs.size() - 1).getLogEntry().getIndex();
	}

	@Getter
	@Setter
	@ToString
	private static class LogEntryReply {

		final CountDownLatch latch;

		final LogEntry logEntry;

		AppendLogReply reply;

		public LogEntryReply(CountDownLatch latch, LogEntry logEntry) {
			this.latch = latch;
			this.logEntry = logEntry;
		}
	}

	/**
	 * 追加日志
	 * 
	 * @param logEntry
	 *            日志
	 * @return 是否成功
	 */
	public AppendLogReply appendLog(LogEntry logEntry) {
		CountDownLatch latch = new CountDownLatch(1);
		LogEntryReply entry = new LogEntryReply(latch, logEntry);
		try {
			logs.put(logEntry.getIndex(), entry);
			latch.await();
		} catch (InterruptedException e) {
			log.warn("appendLog interrupted, e: {}", e.toString());
		}
		return entry.getReply();
	}
}
