package com.gill.consensus.raftplus.machine;

import lombok.Getter;

/**
 * NodeState
 *
 * @author gill
 * @version 2023/09/04
 **/
@Getter
public enum RaftEvent {

	/**
	 * 初始化状态机
	 */
	INIT,

	/**
	 * 中止状态机
	 */
	STOP,

	/**
	 * 网络分区
	 */
	NETWORK_PARTITION,

	/**
	 * 接收Leader心跳包超时
	 */
	PING_TIMEOUT,

	/**
	 * 预投票成功
	 */
	PREVOTE_SUCCESS,

	/**
	 * 预投票失败
	 */
	PREVOTE_FAILED,

	/**
	 * 投票超时
	 */
	VOTE_FAILED,

	/**
	 * 投票成功成为leader
	 */
	TO_LEADER,

	/**
	 * 收到高任期leader的心跳包
	 */
	ACCEPT_LEADER,

	/**
	 * 强制转换成FOLLOWER
	 */
	FORCE_FOLLOWER;
}
