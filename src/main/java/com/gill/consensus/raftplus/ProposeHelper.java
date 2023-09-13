package com.gill.consensus.raftplus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.gill.consensus.common.Util;
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

	private final Supplier<ExecutorService> supplier;

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

	public ProposeHelper(Supplier<ExecutorService> supplier) {
		this.supplier = supplier;
	}

	public void setFollowerProxies(List<NodeProxy> followerProxies) {
		this.followerProxies = followerProxies;
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
	public void stop() {
		List<NodeProxy> proxies = followerProxies;
		followerProxies = Collections.emptyList();
		proxies.forEach(NodeProxy::stop);
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
		boolean success = Util.majorityCall(followerProxies, proxy -> proxy.appendLog(logEntry), Reply::isSuccess,
				supplier.get(), "propose");
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
