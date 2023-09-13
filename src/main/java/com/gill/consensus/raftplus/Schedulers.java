package com.gill.consensus.raftplus;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gill.consensus.raftplus.config.RaftConfig;

import cn.hutool.core.util.RandomUtil;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Schedulers
 *
 * @author gill
 * @version 2023/09/05
 **/
@Getter
public class Schedulers {

	@Getter(AccessLevel.NONE)
	private final Lock timeoutLock = new ReentrantLock();

	private ScheduledExecutorService timeoutScheduler;

	@Getter(AccessLevel.NONE)
	private final Lock heartbeatLock = new ReentrantLock();

	private ScheduledExecutorService heartbeatScheduler;

	/**
	 * set
	 *
	 * @param runnable
	 *            r
	 * @param config
	 *            config
	 */
	public void setTimeoutScheduler(Runnable runnable, RaftConfig config) {
		timeoutLock.lock();
		try {
			if (this.timeoutScheduler == null) {
				this.timeoutScheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "raft-plus-follower"));
			}
			this.timeoutScheduler.scheduleAtFixedRate(runnable, 0,
					config.getTimeoutInterval() + RandomUtil.randomLong(config.getTimeoutRandomFactor()),
					TimeUnit.MILLISECONDS);
		} finally {
			timeoutLock.unlock();
		}
	}

	/**
	 * 清除定时任务
	 */
	public void clearTimeoutScheduler() {
		ScheduledExecutorService tmp = this.timeoutScheduler;
		timeoutLock.lock();
		try {
			this.timeoutScheduler = null;
		} finally {
			timeoutLock.unlock();
		}
		if (tmp != null) {
			tmp.shutdown();
		}
	}

	/**
	 * set
	 *
	 * @param runnable
	 *            r
	 * @param config
	 *            config
	 */
	public synchronized void setHeartbeatScheduler(Runnable runnable, RaftConfig config) {
		heartbeatLock.lock();
		try {
			if (this.heartbeatScheduler == null) {
				this.heartbeatScheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "raft-plus-leader"));;
			}
			this.heartbeatScheduler.scheduleAtFixedRate(runnable, 0, config.getHeartbeatInterval(),
					TimeUnit.MILLISECONDS);
		} finally {
			heartbeatLock.unlock();
		}
	}

	/**
	 * 清除定时任务
	 */
	public void clearHeartbeatScheduler() {
		ScheduledExecutorService tmp = this.heartbeatScheduler;
		heartbeatLock.lock();
		try {
			this.heartbeatScheduler = null;
		} finally {
			heartbeatLock.unlock();
		}
		if (tmp != null) {
			tmp.shutdown();
		}
	}
}
