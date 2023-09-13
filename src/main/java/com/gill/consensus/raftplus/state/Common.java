package com.gill.consensus.raftplus.state;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.machine.RaftEventParams;

import lombok.extern.slf4j.Slf4j;

/**
 * Common
 *
 * @author gill
 * @version 2023/09/06
 **/
@Slf4j
public class Common {

	/**
	 * 初始化follower
	 *
	 * @param self
	 *            节点
	 */
	public static void stop(Node self) {
		self.getThreadPools().setClusterPool(null);
		self.getThreadPools().setApiPool(null);
	}

	/**
	 * 处理版本号更高的leader心跳通知
	 * 
	 * @param self
	 *            节点
	 * @param params
	 *            参数
	 */
	public static void acceptHigherLeader(Node self, RaftEventParams params) {
		self.setTermAndVotedFor(params.getTerm(), params.getVotedFor());
	}
}
