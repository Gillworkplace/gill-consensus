package com.gill.consensus.paxos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.ToString;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.paxos.model.Proposal;
import com.gill.consensus.common.Remote;
import com.gill.consensus.common.Util;

import lombok.Getter;

/**
 * Learner
 *
 * @author gill
 * @version 2023/07/31
 **/
@Getter
@Scope("prototype")
@Component
@ToString
public class Learner {

	private final int id = 1000 + Util.getId();

	private final Map<Integer, Proposal> dict = new ConcurrentHashMap<>();

	/**
	 * save
	 *
	 * @param proposal
	 *            proposal
	 * @return boolean
	 */
	@Remote
	public boolean learn(Proposal proposal) {
		boolean[] success = new boolean[1];
		dict.compute(proposal.proposerId, (key, oldProposal) -> {
			if (oldProposal == null || oldProposal.proposalNumber < proposal.proposalNumber) {
				success[0] = true;
				return proposal;
			} else {
				return oldProposal;
			}
		});
		return success[0];
	}
}
