package com.gill.consensus.raftplus.service;

/**
 * ClusterService
 *
 * @author gill
 * @version 2023/09/04
 **/
public interface ClusterService extends NodeService {

	/**
	 * 提案
	 *
	 * @param command
	 *            更新操作
	 * @return 日志索引位置
	 */
	int propose(String command);
}
