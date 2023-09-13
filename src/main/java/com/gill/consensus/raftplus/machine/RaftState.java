package com.gill.consensus.raftplus.machine;

/**
 * RaftState
 *
 * @author gill
 * @version 2023/09/04
 **/
public enum RaftState {

	/**
	 * 未初始化
	 */
	STRANGER,

	/**
	 * 从者
	 */
	FOLLOWER,

	/**
	 * 预候选者
	 */
	PRE_CANDIDATE,

	/**
	 * 候选者
	 */
	CANDIDATE,

	/**
	 * 领导者
	 */
	LEADER
}
