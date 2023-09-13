package com.gill.consensus.raftplus.state;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.gill.consensus.common.Util;
import com.gill.consensus.raftplus.LogManager;
import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.ProposeHelper;
import com.gill.consensus.raftplus.entity.AppendLogEntriesParam;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.machine.RaftEvent;
import com.gill.consensus.raftplus.machine.RaftEventParams;

import com.gill.consensus.raftplus.service.InnerNodeService;
import lombok.extern.slf4j.Slf4j;

/**
 * Leader
 *
 * @author gill
 * @version 2023/09/05
 **/
@Slf4j
public class Leader {

	/**
	 * 启动心跳定时任务
	 *
	 * @param self
	 *            节点
	 */
	public static void startHeartbeatSchedule(Node self) {
		ExecutorService heartbeatPool = self.getThreadPools().getClusterPool();
		self.getSchedulers().setHeartbeatScheduler(() -> {
			List<InnerNodeService> followers = self.getFollowers();
			boolean success = Util.majorityCall(followers, follower -> doHeartbeat(self, follower), Reply::isSuccess, heartbeatPool,
					"heartbeat");
			if (!success) {
				self.publishEvent(RaftEvent.NETWORK_PARTITION, RaftEventParams.builder().term(self.getTerm()).build());
			}
		}, self.getConfig());
	}

	private static Reply doHeartbeat(Node self, InnerNodeService follower) {
		int nodeId = self.getID();
		long term = self.getTerm();
		AppendLogEntriesParam param = AppendLogEntriesParam.builder(nodeId, term).build();
		return follower.doAppendLogEntries(param);
	}

	/**
	 * 停止心跳定时任务
	 *
	 * @param self
	 *            节点
	 */
	public static void stopHeartbeatSchedule(Node self) {
		self.getSchedulers().clearHeartbeatScheduler();
	}

	/**
	 * 发送noOp指令
	 * 
	 * @param self
	 *            节点
	 */
	public static void noOp(Node self) {
		self.propose("no-op");
	}

	/**
	 * 初始化leader
	 * 
	 * @param self
	 *            节点
	 */
	public static void init(Node self) {
		ProposeHelper proposeHelper = self.getProposeHelper();
		LogManager logManager = self.getLogManager();
		int lastLogIdx = logManager.lastLog().getValue();
		proposeHelper.start(self, self.getFollowers(), lastLogIdx);
	}

	/**
	 * 停止leader的任务
	 * 
	 * @param self
	 *            节点
	 */
	public static void stop(Node self) {
		ProposeHelper proposeHelper = self.getProposeHelper();
		proposeHelper.setFollowerProxies(Collections.emptyList());
	}
}
