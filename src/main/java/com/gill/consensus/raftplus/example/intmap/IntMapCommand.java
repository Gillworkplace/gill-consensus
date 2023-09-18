package com.gill.consensus.raftplus.example.intmap;

import java.util.Map;
import java.util.function.BiFunction;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MapCommand
 *
 * @author gill
 * @version 2023/09/07
 **/
@Slf4j
@Builder(builderMethodName = "innerBuilder")
@Getter
@Setter
@ToString
public class IntMapCommand {

	public enum Type {

		/**
		 * 查询操作
		 */
		GET((map, command) -> String.valueOf(map.get(command.getKey()))),

		/**
		 * 设置操作
		 */
		PUT((map, command) -> {
			map.put(command.getKey(), command.getValue());
			return "OK";
		});

		private final BiFunction<Map<String, Integer>, IntMapCommand, String> handler;

		Type(BiFunction<Map<String, Integer>, IntMapCommand, String> handler) {
			this.handler = handler;
		}
	}

	private Type type;

	private String key;

	private Integer value;

	/**
	 * 执行命令
	 *
	 * @param map
	 *            map
	 * @param command
	 *            命令
	 * @return 结果
	 */
	public String execute(Map<String, Integer> map, IntMapCommand command) {
		return this.type.handler.apply(map, command);
	}

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
