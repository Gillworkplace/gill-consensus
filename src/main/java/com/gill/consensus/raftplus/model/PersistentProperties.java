package com.gill.consensus.raftplus.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * PersistentProperties
 *
 * @author gill
 * @version 2023/09/13
 **/
@Getter
@Setter
@ToString
public class PersistentProperties {

    private long term = 0L;

    private Integer votedFor = null;

    public void set(PersistentProperties properties) {
        term = properties.getTerm();
        votedFor = properties.getVotedFor();
    }
}
