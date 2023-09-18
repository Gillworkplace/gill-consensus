package com.gill.consensus.raftplus.apis;

/**
 * IntMapReply
 *
 * @author gill
 * @version 2023/09/18
 **/
public abstract class BaseReply {

    private Integer idx;

    public void setIdx(Integer idx) {
        this.idx = idx;
    }
}
