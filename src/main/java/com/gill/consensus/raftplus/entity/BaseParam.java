package com.gill.consensus.raftplus.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
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
public abstract class BaseParam {

    private int nodeId;

    private long term;
}
