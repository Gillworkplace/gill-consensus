package com.gill.consensus.raft;

import lombok.Getter;

/**
 * AbstractState
 *
 * @author gill
 * @version 2023/08/02
 **/
@Getter
public abstract class AbstractState {

    protected Node node;

    public AbstractState(Node node) {
        this.node = node;
        node.setState(this);
    }

    public abstract AbstractState start();

    public abstract AbstractState stop();

    public abstract AbstractState upgrade();

    public abstract AbstractState downgrade();

    public abstract boolean propose(int xid, int x);
}
