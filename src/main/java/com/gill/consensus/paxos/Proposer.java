package com.gill.consensus.paxos;

import static com.gill.consensus.common.Util.EXECUTOR_ACCEPTOR;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.paxos.model.Promise;
import com.gill.consensus.paxos.model.Proposal;
import com.gill.consensus.common.Remote;
import com.gill.consensus.common.Util;

import cn.hutool.core.lang.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Proposer
 *
 * @author gill
 * @version 2023/07/31
 **/
@Slf4j
@Getter
@Scope("prototype")
@Component
@ToString(exclude = {"acceptors"})
public class Proposer {

	private final int id = 100000 + Util.getId();

	private final AtomicInteger proposalNum = new AtomicInteger(0);

	@Setter
	private List<Acceptor> acceptors = new ArrayList<>();

	/**
	 * 提出提案
	 *
	 * @param x
	 *            x
	 * @return 提案值是否通过
	 */
	@Remote
	public int propose(int x) {
		int proposalNum = this.proposalNum.incrementAndGet();
		AtomicReference<Pair<Integer, Integer>> acceptedProposal = new AtomicReference<>(Pair.of(0, x));
		if (!prepare(proposalNum, acceptedProposal)) {
			return -2;
		}
		acceptedProposal.set(Pair.of(proposalNum, acceptedProposal.get().getValue()));
		return accept(proposalNum, acceptedProposal);
	}

	private <R> boolean call(Function<Acceptor, R> callFunc, BiConsumer<AtomicInteger, R> postFunc) {
		AtomicInteger success = new AtomicInteger(0);
		CompletableFuture<?>[] cfs = acceptors.stream()
				.map(acceptor -> CompletableFuture.supplyAsync(() -> callFunc.apply(acceptor), EXECUTOR_ACCEPTOR)
						.thenAccept(ret -> postFunc.accept(success, ret)))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(cfs).join();
		return success.get() > acceptors.size() / 2;
	}

	private boolean prepare(int proposalNum, AtomicReference<Pair<Integer, Integer>> maxAcceptedProposal) {

		// Proposer使用最新的Proposal向Acceptors广播Prepare消息
		Function<Acceptor, Promise> callFunc = acceptor -> {
			log.debug("proposalNum prepare {}", proposalNum);
			return acceptor.prepare(proposalNum);
		};

		// Proposer收到超过半数的Acceptor的Promise响应后，并从Promise响应中获取最大的Proposal
		// Number对应的Proposal Value
		BiConsumer<AtomicInteger, Promise> postFunc = (success, promise) -> {
			if (!promise.isReply()) {
				return;
			}
			log.debug("proposalNum prepare post {}, promise: {}", proposalNum, promise);
			success.incrementAndGet();

			// promise 存在已接收的提案
			if (promise.getAcceptedNum() > 0) {
				maxAcceptedProposal.accumulateAndGet(Pair.of(promise.getAcceptedNum(), promise.getAcceptedVal()),
						(p1, p2) -> {
							if (p1.getKey() < p2.getKey()) {
								return p2;
							}
							return p1;
						});
			}
		};

		return call(callFunc, postFunc);
	}

	private int accept(int proposalNum, AtomicReference<Pair<Integer, Integer>> maxAcceptedProposal) {

		// Proposer向Acceptor发起`Accept(Proposal Number, Max Proposal Value)`
		Proposal proposal = new Proposal();
		proposal.proposerId = this.id;
		proposal.proposalNumber = proposalNum;
		proposal.proposalValue = maxAcceptedProposal.get().getValue();
		Function<Acceptor, Boolean> callFunc = acceptor -> {
			log.debug("proposalNum {} accept: {}", proposalNum, proposal);
			return acceptor.accept(proposal);
		};

		// 判断提议是否被批准
		BiConsumer<AtomicInteger, Boolean> postFunc = (success, accepted) -> {
			log.debug("proposalNum {} accept post: {}", proposalNum, accepted);
			if (accepted) {
				success.incrementAndGet();
			}
		};
		if (call(callFunc, postFunc)) {
			return maxAcceptedProposal.get().getValue();
		}
		return -1;
	}
}
