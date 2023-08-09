package com.gill.consensus.paxos;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import com.gill.consensus.BaseTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import cn.hutool.core.lang.Tuple;
import cn.hutool.core.util.RandomUtil;

/**
 * BasicPaxosTest
 *
 * @author gill
 * @version 2023/07/31
 **/
@SpringBootTest
public class BasicPaxosTest extends BaseTest {

	@Test
	public void testOneProposer() throws InterruptedException {
		Tuple tuple = create(1, 1, 1);
		List<Proposer> proposers = tuple.get(0);
		List<Learner> learners = tuple.get(2);
		propose(proposers, true);
		Thread.sleep(500);
		print(learners);
	}

	@Test
	public void testMoreProposer() throws InterruptedException {
		Tuple tuple = create(3, 3, 2);
		List<Proposer> proposers = tuple.get(0);
		List<Learner> learners = tuple.get(2);
		propose(proposers, true);
		Thread.sleep(500);
		print(learners);
	}

	@Test
	public void testSuccessProposals() throws InterruptedException {
		Tuple tuple = create(3, 3, 2);
		List<Proposer> proposers = tuple.get(0);
		List<Learner> learners = tuple.get(2);
		propose(proposers, false);
		Thread.sleep(500);
		print(learners);
	}

	@Test
	public void testConcurrencySameProposer() throws InterruptedException {
		Tuple tuple = create(1, 3, 2);
		List<Proposer> proposers = tuple.get(0);
		List<Acceptor> acceptors = tuple.get(1);
		List<Learner> learners = tuple.get(2);
		AtomicInteger x = new AtomicInteger(16);
		CompletableFuture<?>[] cfs = IntStream.range(0, 5)
				.mapToObj(i -> CompletableFuture.runAsync(() -> propose(proposers, true, x)))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(cfs).join();
		Thread.sleep(500);
		print(learners);
		print(acceptors);
	}

	@Test
	public void testSequenceSameProposer() throws InterruptedException {
		Tuple tuple = create(1, 3, 1);
		List<Proposer> proposers = tuple.get(0);
		List<Acceptor> acceptors = tuple.get(1);
		List<Learner> learners = tuple.get(2);
		AtomicInteger x = new AtomicInteger(16);
		IntStream.range(0, 5).forEach(i -> propose(proposers, true, x));
		Thread.sleep(1000);
		print(learners);
		print(acceptors);
	}

	private Tuple create(int pSize, int aSize, int lSize) {
		List<Proposer> proposers = newList(pSize, Proposer.class);
		List<Acceptor> acceptors = newList(aSize, Acceptor.class);
		List<Learner> learners = newList(lSize, Learner.class);

		proposers.forEach(proposer -> proposer.setAcceptors(acceptors));
		acceptors.forEach(acceptor -> acceptor.setLearners(learners));
		return new Tuple(proposers, acceptors, learners);
	}

	private static void propose(List<Proposer> proposers, boolean once) {
		AtomicInteger x = new AtomicInteger(16);
		if (once) {
			doPropose(proposers, proposer -> {
				int in = x.getAndIncrement();
				System.out.printf("propose x: %d, ret: %d%n", in, proposer.propose(in));
			});
		} else {
			doPropose(proposers, proposer -> {
				int in = x.getAndIncrement();
				int ret;
				while ((ret = proposer.propose(in)) < 0) {
					try {
						Thread.sleep(RandomUtil.randomInt(200));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				System.out.printf("propose x: %d , ret: %d%n", in, ret);
			});
		}
	}

	private static void propose(List<Proposer> proposers, boolean once, AtomicInteger val) {
		final AtomicInteger x = val;
		if (once) {
			doPropose(proposers, proposer -> {
				int in = x.getAndIncrement();
				System.out.printf("propose x: %d, ret: %d%n", in, proposer.propose(in));
			});
		} else {
			doPropose(proposers, proposer -> {
				int in = x.getAndIncrement();
				int ret;
				while ((ret = proposer.propose(in)) < 0) {
					try {
						Thread.sleep(RandomUtil.randomInt(200));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				System.out.printf("propose x: %d , ret: %d%n", in, ret);
			});
		}
	}

	private static void doPropose(List<Proposer> proposers, Consumer<Proposer> func) {
		CompletableFuture<?>[] cfs = proposers.stream()
				.map(proposer -> CompletableFuture.runAsync(() -> func.accept(proposer)))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(cfs).join();
	}

}
