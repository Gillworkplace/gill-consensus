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
	 * 接收Leader心跳包超时
	 */
	PING_TIMEOUT,

	/**
	 * 预投票成功
	 */
	TO_CANDIDATE,

	/**
	 * 投票成功成为leader
	 */
	TO_LEADER,

	/**
	 * 强制转换成FOLLOWER
	 */
	FORCE_FOLLOWER;
}
