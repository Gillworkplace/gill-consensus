package com.gill.consensus.raft.mock;

import com.gill.consensus.common.Remote;
import com.gill.consensus.raft.model.LogEntries;
import com.gill.consensus.raft.model.Reply;
import com.gill.consensus.raft.model.Vote;
import com.gill.consensus.raft.model.VoteReply;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.raft.Node;

/**
 * NodeRemote
 *
 * @author gill
 * @version 2023/08/09
 **/
@Scope("prototype")
@Component("raftNodeRemote")
public class NodeRemote extends Node {

    @Remote
    @Override
    public boolean propose(int xid, int x) {
        return super.propose(xid, x);
    }

    @Remote
    @Override
    public VoteReply requestVote(Vote vote) {
        return super.requestVote(vote);
    }

    @Remote
    @Override
    public Reply appendLogEntries(LogEntries logEntries) {
        return super.appendLogEntries(logEntries);
    }
}
