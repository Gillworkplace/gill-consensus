package com.gill.consensus.raftplus.state;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.gill.consensus.common.Util;
import com.gill.consensus.raftplus.LogManager;
import com.gill.consensus.raftplus.Node;
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
		ExecutorService clusterPool = self.getThreadPools().getClusterPool();
		List<InnerNodeService> followers = self.getFollowers();
		long nextTerm = self.increaseTerm(params.getTerm(), self.getID());

		// 该节点已投票，直接返回失败
		if (nextTerm == -1) {
			self.publishEvent(RaftEvent.VOTE_FAILED, RaftEventParams.builder().term(self.getTerm()).build());
			return;
		}
		log.info("node: {} vote for self when term: {}", self.getID(), nextTerm);
		boolean success = Util.majorityCall(followers, follower -> doVote(self, follower, nextTerm), Reply::isSuccess,
				clusterPool, "vote");
		if (success) {
			self.publishEvent(RaftEvent.TO_LEADER, RaftEventParams.builder().term(nextTerm).build());
			log.debug("node: {} become to leader when term is {}", self.getID(), nextTerm);
		} else {
			self.publishEvent(RaftEvent.VOTE_FAILED, RaftEventParams.builder().term(self.getTerm()).build());
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
