package com.gill.consensus.raftplus.state;

import java.util.List;
import java.util.concurrent.ExecutorService;

import cn.hutool.core.util.RandomUtil;
import com.gill.consensus.raftplus.LogManager;
import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.PreVoteParam;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.machine.RaftEvent;
import com.gill.consensus.raftplus.machine.RaftEventParams;
import com.gill.consensus.raftplus.service.InnerNodeService;

import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;

/**
 * PreCandidate
 *
 * @author gill
 * @version 2023/09/05
 **/
@Slf4j
public class PreCandidate {

	/**
	 * 预投票
	 *
	 * @param self
	 *            发起节点
	 * @param params
	 *            params
	 */
	public static void preVote(Node self, RaftEventParams params) {
		long pTerm = params.getTerm();
		log.info("start to pre-vote when term: {}", pTerm);
		ExecutorService clusterPool = self.getThreadPools().getClusterPool();
		List<InnerNodeService> followers = self.getFollowers();
		boolean success = Utils.majorityCall(followers, follower -> doPreVote(self, follower, params), Reply::isSuccess,
				clusterPool, "pre-vote");
		if (success) {
			self.publishEvent(RaftEvent.TO_CANDIDATE, new RaftEventParams(pTerm));
		} else {
			log.debug("pre-vote failed when term: {}", pTerm);

			// 增加随机时间
			Utils.sleepQuietly(RandomUtil.randomInt(10));
			self.stepDown();
		}
	}

	private static Reply doPreVote(Node self, InnerNodeService follower, RaftEventParams params) {
		LogManager logManager = self.getLogManager();
		Pair<Long, Integer> lastLog = logManager.lastLog();
		int nodeId = self.getID();
		long term = params.getTerm();
		long lastLogTerm = lastLog.getKey();
		int lastLogIdx = lastLog.getValue();
		return follower.preVote(new PreVoteParam(nodeId, term, lastLogTerm, lastLogIdx));
	}
}
