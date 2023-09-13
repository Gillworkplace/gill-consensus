package com.gill.consensus.raftplus.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Reply
 *
 * @author gill
 * @version 2023/08/18
 **/
@Getter
@Setter
@AllArgsConstructor
@ToString
public class Reply {

	private boolean success;

	private long term;
}
