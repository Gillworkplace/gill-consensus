package com.gill.consensus.raftplus.state;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gill.consensus.common.Util;
import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.config.RaftConfig;
import com.gill.consensus.raftplus.machine.RaftEvent;
import com.gill.consensus.raftplus.machine.RaftEventParams;
import com.gill.consensus.raftplus.service.InnerNodeService;

import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;

/**
 * Follower
 *
 * @author gill
 * @version 2023/09/05
 **/
@Slf4j
public class Follower {

	/**
	 * 启动心跳检测超时定时任务
	 * 
	 * @param self
	 *            节点
	 */
	public static void startTimeoutScheduler(Node self) {
		log.debug("starting timeout scheduler");
		RaftConfig config = self.getConfig();
		self.getSchedulers().setTimeoutScheduler(() -> {
			RaftConfig newConfig = self.getConfig();
			Pair<Long, Long> pair = self.getHeartbeatState().get();
			long lastHeartbeatTimestamp = pair.getValue();
			long now = System.currentTimeMillis();
			long diff = now - lastHeartbeatTimestamp;
			if (diff <= newConfig.getBaseTimeoutInterval() + self.getPriority() * 100L) {
				return;
			}
			self.unstable();
			self.publishEvent(RaftEvent.PING_TIMEOUT, new RaftEventParams(self.getTerm()));
		}, config.getCheckTimeoutInterval() + self.getPriority() * 2L, self.getID());
	}

	/**
	 * 停止心跳检测超时定时任务
	 * 
	 * @param self
	 *            节点
	 */
	public static void stopTimeoutScheduler(Node self) {
		log.debug("stopping timeout scheduler");
		self.getSchedulers().clearTimeoutScheduler();
	}

	/**
	 * 初始化follower
	 * 
	 * @param self
	 *            节点
	 */
	public static void init(Node self) {
		int nodeId = self.getID();
		List<InnerNodeService> followers = self.getFollowers();
		self.getThreadPools()
				// .setClusterPool(new ThreadPoolExecutor(followers.size() + 1, followers.size()
				// + 1, 0,
				.setClusterPool(new ThreadPoolExecutor(followers.size() + 1, 100, 0, TimeUnit.MILLISECONDS,
						new LinkedBlockingQueue<>(Collections.emptyList()), r -> new Thread(r, "cluster-" + nodeId),
						(r, executor) -> log.warn("Node {} discards extra heartbeat thread", nodeId)));
		self.getThreadPools().setApiPool(new ThreadPoolExecutor(Util.CPU_CORES * 2 + 1, Util.CPU_CORES * 4 + 2, 600,
				TimeUnit.SECONDS, new LinkedBlockingQueue<>(20), r -> new Thread(r, "api-" + nodeId)));
	}
}
