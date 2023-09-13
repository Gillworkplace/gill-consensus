package com.gill.consensus.raftplus.service;

import java.util.List;

import com.gill.consensus.raftplus.Node;

/**
 * NodeService
 *
 * @author gill
 * @version 2023/09/04
 **/
public interface NodeService {

	/**
	 * 是否准备完成
	 * 
	 * @return 是否准备完成
	 */
	boolean ready();

	/**
	 * 启动节点
	 *
	 * @param nodes
	 *            集群节点
	 */
	void start(List<? extends Node> nodes);

	/**
	 * 停止节点
	 */
	void stop();

	/**
	 * 清除节点数据
	 */
	void clear();
}
