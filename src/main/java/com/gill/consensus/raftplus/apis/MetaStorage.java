package com.gill.consensus.raftplus.apis;

import com.gill.consensus.raftplus.PersistentProperties;

/**
 * MetaStorage
 *
 * @author gill
 * @version 2023/09/11
 **/
public interface MetaStorage {

	/**
	 * 持久化元数据
	 * 
	 * @param properties
	 *            属性
	 */
	void write(PersistentProperties properties);

	/**
	 * 读属性
	 * 
	 * @return properties
	 */
	PersistentProperties read();
}
