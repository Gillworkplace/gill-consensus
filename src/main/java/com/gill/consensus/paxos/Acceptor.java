package com.gill.consensus.paxos;

import static com.gill.consensus.common.Util.EXECUTOR_LEARNER;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.paxos.model.Promise;
import com.gill.consensus.paxos.model.Proposal;
import com.gill.consensus.common.Remote;
import com.gill.consensus.common.Util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Acceptor
 *
 * @author gill
 * @version 2023/07/31
 **/
@Getter
@Scope("prototype")
@Component
@ToString(exclude = {"learners", "prepareLock", "acceptLock"})
public class Acceptor {

	private final int id = 10000 + Util.getId();

	private volatile int proposalNum = 0;

	private int acceptedProposalNum = -1;

	private int acceptedProposalVal = -1;

	@Setter
	private List<Learner> learners = new ArrayList<>();

	private transient final Lock prepareLock = new ReentrantLock();

	private transient final Lock acceptLock = new ReentrantLock();

	/**
	 * prepare
	 *
	 * @param proposalNum
	 *            proposalNum
	 * @return Promise
	 */
	@Remote
	public Promise prepare(int proposalNum) {
		prepareLock.lock();
		try {
			if (proposalNum <= this.proposalNum) {
				return Promise.FAILED;
			}

			// 如果Prepare消息的Proposal Number大于之前所有的提案，则返回Promise消息，
			// 承诺不会再接受任何编号小于Proposal Number的提案`Promise(Proposal Number, null, null)`。
			// 另外，如果接受者之前接受了某个提案，那么Promise响应还会将前一次的Proposal Number和Proposal Value一起发送给
			// Proposer `Promise(Proposal Number, Accepted Proposal Number, Accepted
			// Proposal Value）`
			this.proposalNum = proposalNum;
			if (this.acceptedProposalNum == -1) {
				return new Promise(proposalNum);
			}
			return new Promise(proposalNum, this.acceptedProposalNum, this.acceptedProposalVal);
		} finally {
			prepareLock.unlock();
		}
	}

	/**
	 * accept
	 *
	 * @param proposal
	 *            proposal
	 * @return boolean
	 */
	@Remote
	public boolean accept(Proposal proposal) {
		acceptLock.lock();
		try {
			if (proposalNum < this.proposalNum) {
				return false;
			}

			// 如果Proposal Number大于Acceptor中已见过的Proposal Number，则接受该Proposal并更新保存Proposal
			this.proposalNum = proposal.proposalNumber;
			this.acceptedProposalNum = proposal.proposalNumber;
			this.acceptedProposalVal = proposal.proposalValue;
			AtomicInteger success = new AtomicInteger(0);
			CompletableFuture<?>[] cfs = learners.stream().map(learner -> CompletableFuture
					.supplyAsync(() -> learner.learn(proposal), EXECUTOR_LEARNER).thenAccept(ret -> {
						if (ret) {
							success.incrementAndGet();
						}
					})).toArray(CompletableFuture[]::new);
			CompletableFuture.allOf(cfs).join();
			return success.get() > learners.size() / 2;
		} finally {
			acceptLock.unlock();
		}
	}
}
