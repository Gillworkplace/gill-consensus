package com.gill.consensus.raft;

import static com.gill.consensus.common.Util.CPU_CORES;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.hutool.core.util.RandomUtil;
import com.gill.consensus.common.Util;
import com.gill.consensus.raft.model.LogEntries;
import com.gill.consensus.raft.model.LogEntry;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Leader
 *
 * @author gill
 * @version 2023/08/02
 **/
@Slf4j
@Getter
@Setter
public class Leader extends AbstractState {

	private ExecutorService pool = new ThreadPoolExecutor(4 * CPU_CORES + 1, 4 * CPU_CORES + 1, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "raft-leader"));

	private ScheduledExecutorService scheduler;

	private volatile long broadcastTime = 0L;

	public Leader(Node node) {
		super(node);
	}

	@Override
	public String toString() {
		return "Leader{" + "broadcastTime=" + broadcastTime + '}';
	}

	@Override
	public AbstractState start() {
		scheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "raft-leader"));

		// no-op广播
		if(!proposeNoOp()) {
			log.info("node {} no-op broadcast failed", node.getId());
			return this.downgrade();
		}

		log.info("node {} no-op broadcast success and schedule broadcast heartbeat now", node.getId());
		scheduler.scheduleAtFixedRate(() -> {
			long now = System.currentTimeMillis();
			if (now - broadcastTime > Custom.INTERVAL) {
				log.trace("node {} broadcast heartbeat", node.getId());
				broadcastTime = now;
				List<Node> followers = Util.getFollowers(node.getNodes(), n -> node.getId() != n.getId());
				boolean ret = Util.majorityCall(followers,
						follower -> follower.appendLogEntries(new LogEntries(node, follower.getCurrentTerm().get())),
						reply -> reply.success, pool, "heartbeat");

				// 不够半数响应时降级为follower
				if (!ret) {
					this.downgrade();
				}
			}
		}, 0, 100, TimeUnit.MILLISECONDS);
		return this;
	}

	private boolean proposeNoOp() {
		return propose(-RandomUtil.randomInt(816), -1);
	}

	@Override
	public AbstractState stop() {
		pool.shutdown();
		pool = null;
		scheduler.shutdown();
		scheduler = null;
		return this;
	}

	@Override
	public AbstractState upgrade() {
		throw new IllegalStateException("leader can not upgrade");
	}

	@Override
	public AbstractState downgrade() {
		this.stop();
		return new Follower(node).start();
	}

	@Override
	public boolean propose(int xid, int x) {
		int logIdx = node.getNextIndex().getAndIncrement();
		LogEntry logEntry = new LogEntry(logIdx, xid, node.getCurrentTerm().get(), x);
		node.getLogs().put(logIdx, logEntry);
		List<NodeProxy> followers = Util.getFollowers(node.getNodesProxy(), proxy -> node.getId() != proxy.getNode().getId());
		boolean ret = Util.majorityCall(followers, proxy -> proxy.appendEntry(logEntry), reply -> reply.success,
				pool, "appendEntry");
		if (ret) {
			node.getCommittedIndex().accumulateAndGet(logIdx, Math::max);
		}
		return ret;
	}
}
