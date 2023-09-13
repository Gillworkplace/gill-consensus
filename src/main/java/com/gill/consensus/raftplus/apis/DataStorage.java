package com.gill.consensus.raftplus.apis;

import com.gill.consensus.raftplus.model.Snapshot;
import com.gill.consensus.raftplus.service.PrintService;

/**
 * Apply
 *
 * @author gill
 * @version 2023/09/07
 **/
public interface DataStorage extends PrintService {

	/**
	 * 获取applyIdx
	 *
	 * @return applyIdx
	 */
	int getApplyIdx();

	/**
	 * 加载数据
	 *
	 * @return applyIdx
	 */
	int loadSnapshot();

	/**
	 * 获取当前快照
	 * 
	 * @return 快照
	 */
	Snapshot getSnapshot();

	/**
	 * 保存数据
	 */
	void saveSnapshot();

	/**
	 * 保存快照
	 * 
	 * @param term
	 *            日志任期
	 * @param applyIdx
	 *            日志idx
	 * @param data
	 *            快照数据
	 */
	void saveSnapshot(long term, int applyIdx, byte[] data);

	/**
	 * 应用命令
	 * 
	 * @param command
	 *            命令
	 * @return 返回结果
	 */
	String apply(String command);

	/**
	 * 应用命令
	 * 
	 * @param logIdx
	 *            日志索引
	 * @param command
	 *            命令
	 * @return 响应
	 */
	String apply(int logIdx, String command);
}
