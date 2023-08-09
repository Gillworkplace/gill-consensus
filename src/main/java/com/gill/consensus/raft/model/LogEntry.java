package com.gill.consensus.raft.model;

import lombok.ToString;

/**
 * LogEntry
 *
 * @author gill
 * @version 2023/08/03
 **/
@ToString
public class LogEntry {

    public int index;

    public int id;

    public int term;

    public int val;

    public boolean committed;

    public LogEntry(int index, int id, int term, int val) {
        this.index = index;
        this.id = id;
        this.term = term;
        this.val = val;
    }
}
