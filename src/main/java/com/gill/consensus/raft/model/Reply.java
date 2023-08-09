package com.gill.consensus.raft.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Reply
 *
 * @author gill
 * @version 2023/08/03
 **/
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Reply {

    public int term;

    public boolean success;
}
