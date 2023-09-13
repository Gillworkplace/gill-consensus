package com.gill.consensus.raftplus.example.intmap;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * MapCommand
 *
 * @author gill
 * @version 2023/09/07
 **/
@Builder(builderMethodName = "innerBuilder")
@Getter
@ToString
public class IntMapCommand {

	public enum Type {

		/**
		 * 查询操作
		 */
		GET,

		/**
		 * 设置操作
		 */
		PUT
	}

	private Type type;

	private String key;

	private int value;

	/**
	 * builder
	 * 
	 * @param type
	 *            type
	 * @param key
	 *            key
	 * @return builder
	 */
	public static IntMapCommandBuilder builder(Type type, String key) {
		return innerBuilder().type(type).key(key);
	}
}
