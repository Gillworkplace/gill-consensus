package com.gill.consensus.paxos.model;

import lombok.Data;

/**
 * Promise
 *
 * @author gill
 * @version 2023/07/31
 **/
@Data
public class Promise {

    public static Promise FAILED = new Promise(false);

    private boolean reply;

    private int proposalNum;

    private int acceptedNum;

    private int acceptedVal;

    private Promise(boolean reply) {
        this.reply = reply;
    }

    public Promise(int proposalNum) {
        this.reply = true;
        this.proposalNum = proposalNum;
    }

    public Promise(int proposalNum, int acceptedNum, int acceptedVal) {
        this.reply = true;
        this.proposalNum = proposalNum;
        this.acceptedNum = acceptedNum;
        this.acceptedVal = acceptedVal;
    }
}
