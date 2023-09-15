package com.gill.consensus.raftplus.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * TermParam
 *
 * @author gill
 * @version 2023/09/13
 **/
@Getter
@SuperBuilder
@AllArgsConstructor
@ToString
public abstract class BaseParam {

	private int nodeId;

	private long term;
}
