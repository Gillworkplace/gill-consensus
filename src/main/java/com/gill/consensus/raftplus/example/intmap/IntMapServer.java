package com.gill.consensus.raftplus.example.intmap;

import java.util.List;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.apis.EmptyLogStorage;
import com.gill.consensus.raftplus.apis.EmptyMetaStorage;

/**
 * MapServer
 *
 * @author gill
 * @version 2023/09/07
 **/
public class IntMapServer {

	private final Node node;

	private final IntMapDataStorage dataStorage = new IntMapDataStorage();

	private final IntMapCommandSerializer serializer = new IntMapCommandSerializer();

	public IntMapServer(int id) {
		this.node = new Node(id, new EmptyMetaStorage(), dataStorage, new EmptyLogStorage());
	}

	/**
	 * 启动
	 * 
	 * @param nodes
	 *            节点
	 */
	public void start(List<? extends Node> nodes) {
		node.start(nodes);
	}

	/**
	 * 停止
	 */
	public void stop() {
		node.stop();
	}

	/**
	 * 设置
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return xid
	 */
	public int set(String key, int value) {
		IntMapCommand command = IntMapCommand.builder(IntMapCommand.Type.PUT, key).value(value).build();
		return node.propose(serializer.serialize(command));
	}

	/**
	 * 获取
	 * 
	 * @param key
	 *            key
	 * @return value
	 */
	public Integer get(String key) {
		return dataStorage.get(key);
	}
}
