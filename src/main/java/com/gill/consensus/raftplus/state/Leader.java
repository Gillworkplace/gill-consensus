package com.gill.consensus.raftplus.state;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.gill.consensus.raftplus.LogManager;
import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.ProposeHelper;
import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.AppendLogEntriesParam;
import com.gill.consensus.raftplus.entity.Reply;
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
		log.debug("starting heartbeat scheduler");
		ExecutorService heartbeatPool = self.getThreadPools().getClusterPool();
		self.getSchedulers().setHeartbeatScheduler(() -> {
			List<InnerNodeService> followers = self.getFollowers();
			log.debug("broadcast heartbeat");
			boolean success = Utils.majorityCall(followers, follower -> doHeartbeat(self, follower), Reply::isSuccess,
					heartbeatPool, "heartbeat");
			if (!success) {
				log.debug("broadcast heartbeat failed");
				self.stepDown();
			}
		}, self.getConfig(), self.getID());
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
		log.debug("stopping heartbeat scheduler");
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
		log.debug("init propose helper");
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
	public static void clear(Node self) {
		log.debug("clear propose helper");
		ProposeHelper proposeHelper = self.getProposeHelper();
		proposeHelper.clear();
	}
}
