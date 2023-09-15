package com.gill.consensus.raftplus.state;

import java.util.List;
import java.util.concurrent.ExecutorService;

import cn.hutool.core.util.RandomUtil;
import com.gill.consensus.raftplus.LogManager;
import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.entity.RequestVoteParam;
import com.gill.consensus.raftplus.machine.RaftEvent;
import com.gill.consensus.raftplus.machine.RaftEventParams;
import com.gill.consensus.raftplus.service.InnerNodeService;

import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;

/**
 * Candidate
 *
 * @author gill
 * @version 2023/09/05
 **/
@Slf4j
public class Candidate {

	/**
	 * 投票
	 *
	 * @param self
	 *            节点
	 * @param params
	 *            params
	 */
	public static void vote(Node self, RaftEventParams params) {
		long pTerm = params.getTerm();
		long nextTerm = self.electSelf(pTerm);

		// 该节点已投票，直接返回失败
		if (nextTerm == -1) {
			log.debug("vote for self failed when pTerm: {}, cTerm: {}", pTerm, self.getTerm());
			self.stepDown();
			return;
		}
		log.info("vote for self when term: {}", nextTerm);
		ExecutorService clusterPool = self.getThreadPools().getClusterPool();
		List<InnerNodeService> followers = self.getFollowers();
		boolean success = Utils.majorityCall(followers, follower -> doVote(self, follower, nextTerm), Reply::isSuccess,
				clusterPool, "vote");
		if (success) {
			self.publishEvent(RaftEvent.TO_LEADER, new RaftEventParams(nextTerm));
		} else {
			log.debug("request vote failed when term: {}", nextTerm);

			// 增加随机时间
			Utils.sleepQuietly(RandomUtil.randomInt(10));
			self.stepDown();
		}
	}

	private static Reply doVote(Node self, InnerNodeService follower, long term) {
		LogManager logManager = self.getLogManager();
		Pair<Long, Integer> lastLog = logManager.lastLog();
		int nodeId = self.getID();
		long lastLogTerm = lastLog.getKey();
		int lastLogIdx = lastLog.getValue();
		return follower.requestVote(new RequestVoteParam(nodeId, term, lastLogTerm, lastLogIdx));
	}
}
