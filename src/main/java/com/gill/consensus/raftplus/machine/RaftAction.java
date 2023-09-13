package com.gill.consensus.raftplus.machine;

import java.util.function.BiConsumer;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.state.Candidate;
import com.gill.consensus.raftplus.state.Common;
import com.gill.consensus.raftplus.state.Follower;
import com.gill.consensus.raftplus.state.Leader;
import com.gill.consensus.raftplus.state.PreCandidate;

/**
 * RaftAction
 *
 * @author gill
 * @version 2023/09/04
 **/
public enum RaftAction {

	/**
	 * 空处理
	 */
	EMPTY((node, params) -> {
	}),

	/**
	 * 初始化
	 */
	INIT((node, params) -> Follower.init(node)),

	/**
	 * 停止
	 */
	STOP((node, params) -> Common.stop(node)),

	/**
	 * 收到leader心跳包
	 */
	ACCEPT_LEADER(Common::acceptHigherLeader),

	/**
	 * 成为follower
	 */
	POST_FOLLOWER((node, params) -> Follower.startTimeoutSchedule(node)),

	/**
	 * 移除follower定时任务
	 */
	REMOVE_FOLLOWER_SCHEDULER((node, params) -> Follower.stopTimeoutSchedule(node)),

	/**
	 * 成为预候选者
	 */
	TO_PRE_CANDIDATE(PreCandidate::preVote),

	/**
	 * 成为候选者
	 */
	POST_CANDIDATE(Candidate::vote),

	/**
	 * 成为Leader
	 */
	POST_LEADER((node, params) -> {
		Leader.init(node);
		Leader.startHeartbeatSchedule(node);
//		Leader.noOp(node);
	}),

	/**
	 * 移除follower定时任务
	 */
	REMOVE_LEADER_SCHEDULER((node, params) -> {
		Leader.stopHeartbeatSchedule(node);
		Leader.stop(node);
	});

	private final BiConsumer<Node, RaftEventParams> func;

	RaftAction(BiConsumer<Node, RaftEventParams> func) {
		this.func = func;
	}

	/**
	 * 动作
	 *
	 * @param node
	 *            节点
	 * @param params
	 *            参数
	 */
	public void action(Node node, RaftEventParams params) {
		func.accept(node, params);
	}

}
