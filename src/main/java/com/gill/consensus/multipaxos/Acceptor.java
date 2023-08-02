package com.gill.consensus.multipaxos;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.multipaxos.model.Accept;
import com.gill.consensus.multipaxos.model.Accepted;
import com.gill.consensus.multipaxos.model.Prepare;
import com.gill.consensus.multipaxos.model.Promise;
import com.gill.consensus.multipaxos.model.Success;

import cn.hutool.core.lang.Pair;
import lombok.extern.slf4j.Slf4j;

import static com.gill.consensus.common.Util.CHOSEN;

/**
 * Acceptor
 *
 * @author gill
 * @version 2023/08/01
 **/
@Slf4j
@Scope("prototype")
@Component("multiAcceptor")
public class Acceptor extends AbstractState {

	private ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(2,
			r -> new Thread(r, "multi-acceptor"));

	private transient final Lock prepareLock = new ReentrantLock();

	private transient final Lock acceptLock = new ReentrantLock();

	private volatile int minProposal;

	public Acceptor(Node node) {
		super(node);
		node.init();
	}

	@Override
	public AbstractState nextState() {
		return new Leader(node);
	}

	@Override
	public void init() {
		scheduler = new ScheduledThreadPoolExecutor(2, r -> new Thread(r, "multi-acceptor"));
		scheduler.scheduleAtFixedRate(() -> {
			Node maxNode = node.findMaxNode();

			// 如果本机节点ID最大则升级为leader
			if (maxNode == node) {
				nextState();
				stop();
			}
			node.setLeader(maxNode);
		}, 0L, 100L, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop() {
		scheduler.shutdown();
		scheduler = null;
	}

	/**
	 * 请求重定向，demo使用转发
	 *
	 * @param x
	 *            x
	 * @return 提议是否成功
	 */
	@Override
	public boolean propose(int x) {
		return node.getLeader().propose(x);
	}

	/**
	 * prepare
	 *
	 * @param prepare
	 *            prepare
	 * @return Promise
	 */
	public Promise prepare(Prepare prepare) {

		// 只接受大于等于当前提议编号的提案
		if (prepare.proposalNum < minProposal) {
			log.debug("refuse prepare {}, minProposal {}", prepare, minProposal);
			return null;
		}
		prepareLock.lock();
		try {
			minProposal = prepare.proposalNum;
			Promise promise = new Promise();

			// 遍历大于或等于请求参数index的日志条目，如果之后没有接受过任何值，那么noMoreAccepted等于True，否则为False
			if (lastLogIndex.get() < prepare.logIdx) {
				promise.noMoreAccepted = true;
			} else {
				Pair<Integer, Integer> pair = node.getLogs().get(prepare.logIdx);
				promise.acceptedNum = pair.getKey();
				promise.acceptedVal = pair.getValue();
				log.debug("find accepted proposal {}, promise {}", prepare, promise);
			}
			return promise;
		} finally {
			prepareLock.unlock();
		}
	}

	/**
	 * accept
	 * 
	 * @param accept
	 *            accept
	 * @return Accepted
	 */
	public Accepted accept(Accept accept) {
		Accepted accepted = new Accepted();
		if (accept.proposalNum < minProposal) {

			// 返回Acceptor的minProposal，让Leader更新提案编号
			log.debug("refuse accept {}, minProposal {}", accept, minProposal);
			accepted.ok = false;
			accepted.proposalNum = minProposal;
			accepted.firstUnchosenIndex = firstUnchosenIndex;
			return accepted;
		}
		acceptLock.lock();
		try {

			// 接受更新操作
			minProposal = accept.proposalNum;
			ConcurrentSkipListMap<Integer, Pair<Integer, Integer>> logs = node.getLogs();
			logs.put(accept.logIdx, Pair.of(accept.proposalNum, accept.proposalVal));

			// 检查小于accept.firstUnchosenIndex的日志条目是否已批准
			int i;
			for (i = firstUnchosenIndex; i <= accept.firstUnchosenIndex; i++) {
				Pair<Integer, Integer> pair = logs.get(i);

				// 如果发现acceptedProposal[i]的提案编号等于accept的提案编号则设为已批准
				if (pair == null || notAcceptedNumEquals(accept.proposalNum, pair.getKey())) {
					log.debug("next firstUnchosenIndex {}, pair: {}", i, pair);
					break;
				}
				logs.put(i, Pair.of(CHOSEN, pair.getValue()));
			}
			firstUnchosenIndex = i;
			accepted.ok = true;
			accepted.proposalNum = accept.proposalNum;
			accepted.firstUnchosenIndex = firstUnchosenIndex;
			return accepted;
		} finally {
			acceptLock.unlock();
		}
	}

	private static boolean notAcceptedNumEquals(int proposalNum, int acceptedNum) {
		return acceptedNum != CHOSEN && acceptedNum != proposalNum;
	}

	/**
	 * success 幂等性操作 无需加锁
	 * 
	 * @param success
	 *            success
	 * @return Integer
	 */
	public Integer success(Success success) {
		ConcurrentSkipListMap<Integer, Pair<Integer, Integer>> logs = node.getLogs();
		logs.put(success.logIdx, Pair.of(CHOSEN, success.val));

		// 下一个firstUnchosenIndex的位置
		for (int i = firstUnchosenIndex; i <= logs.lastKey(); i++) {
			Pair<Integer, Integer> pair = logs.get(i);

			// 如果发现acceptedProposal[i]的提案编号等于accept的提案编号则设为已批准
			if (pair == null || pair.getKey() != CHOSEN) {
				log.debug("next success firstUnchosenIndex {}, pair: {}", i, pair);
				firstUnchosenIndex = i;
				return i;
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return "Acceptor{" + "minProposal=" + minProposal + ", firstUnchosenIndex=" + firstUnchosenIndex + '}';
	}
}
