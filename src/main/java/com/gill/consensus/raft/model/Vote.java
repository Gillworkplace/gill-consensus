package com.gill.consensus.raft.model;

import lombok.AllArgsConstructor;
import lombok.ToString;

/**
 * Vote
 *
 * @author gill
 * @version 2023/08/02
 **/
@AllArgsConstructor
@ToString
public class Vote {

    public int term;

    public int id;

    public int lastLogIndex;

    public int lastLogTerm;
}
