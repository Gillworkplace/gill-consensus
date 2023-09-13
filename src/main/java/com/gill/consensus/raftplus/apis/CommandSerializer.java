package com.gill.consensus.raftplus.apis;

/**
 * CommandParser
 *
 * @author gill
 * @version 2023/09/07
 **/
public interface CommandSerializer<T> {

	/**
	 * 序列化command
	 *
	 * @param command
	 *            命令
	 * @return 序列化的命令
	 */
	String serialize(T command);

	/**
	 * 反序列化 command
	 * 
	 * @param commandStr
	 *            str
	 * @return command
	 */
	T deserialize(String commandStr);
}
