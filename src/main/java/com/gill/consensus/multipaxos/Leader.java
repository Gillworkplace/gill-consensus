package com.gill.consensus.multipaxos;

import static com.gill.consensus.common.Util.CHOSEN;
import static com.gill.consensus.common.Util.CPU_CORES;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.common.Util;
import com.gill.consensus.multipaxos.model.Accept;
import com.gill.consensus.multipaxos.model.Accepted;
import com.gill.consensus.multipaxos.model.Prepare;
import com.gill.consensus.multipaxos.model.Promise;
import com.gill.consensus.multipaxos.model.Success;

import cn.hutool.core.lang.Pair;
import lombok.extern.slf4j.Slf4j;

/**
 * Leader
 *
 * @author gill
 * @version 2023/08/01
 **/
@Slf4j
@Scope("prototype")
@Component("multiLeader")
public class Leader extends AbstractState {

	private ExecutorService pool = new ThreadPoolExecutor(CPU_CORES + 1, CPU_CORES + 1, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<>(), r -> new Thread(r, "leader-pool"));

	private ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(2, r -> new Thread(r, "multi-leader"));

	private Lock prepareLock = new ReentrantLock();

	private final AtomicInteger maxRound = new AtomicInteger(0);

	private final AtomicInteger nextIndex = new AtomicInteger(1);

	private volatile boolean prepared = false;

	public Leader(Node node) {
		super(node);
		node.setLeader(node);
		node.init();
	}

	@Override
	public AbstractState nextState() {
		return new Acceptor(node);
	}

	@Override
	public void init() {
		scheduler = new ScheduledThreadPoolExecutor(2, r -> new Thread(r, "multi-leader"));
		scheduler.scheduleAtFixedRate(() -> {
			Node maxNode = node.findMaxNode();

			// 如果发现有节点ID大于本机则降级为Acceptor
			if (maxNode != node) {
				node.setLeader(maxNode);
				nextState();
				stop();
			}
		}, 0L, 100L, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop() {
		scheduler.shutdown();
		scheduler = null;
	}

	@Override
	public boolean propose(int x) {
		int val = 0;
		int index;
		while (x != val) {
			int proposalNum;

			// prepare阶段
			if (prepared) {

				// 跳过prepare阶段
				// (1) 如果收到请求的领导者的prepared等于True，则将领导者的index赋值为nextIndex的值，
				// 并执行nextIndex++，然后直接跳转到(6)
				index = nextIndex.getAndIncrement();
				proposalNum = proposalNum(this.maxRound.get());
				val = x;
			} else {

				// (2) 否则，执行index = firstUnchosenIndex, nextIndex = index + 1;
				index = firstUnchosenIndex;
				nextIndex.set(index + 1);

				// (3) 领导者的maxRound自增
				proposalNum = proposalNum(this.maxRound.incrementAndGet());

				// (4) 第一阶段，领导者广播Prepare(n, index)请求给所有的接受者
				AtomicReference<Pair<Integer, Integer>> maxProposal = prepare(x, index, proposalNum);
				if (maxProposal == null) {
					return false;
				}
				val = maxProposal.get().getValue();
				if (val != x) {
					log.debug("repairing old data, idx: {}, proposalNum: {}, val: {}", index, proposalNum, val);
				}
			}

			// (6) 第二阶段，领导者广播Accept(index, n, v)请求到所有接受者。
			// accept阶段
			if (accept(val, index, proposalNum)) {

				// (8) 一旦收到超过半数接受者的成功响应，则修改logs[accepted.firstUnchosenIndex]为最新值
				success(val, index);
			}

			// (9) 如果已批准的提案值v 等于客户端请求的inputValue，则返回true，否则跳转值(1)
		}
		return true;
	}

	private AtomicReference<Pair<Integer, Integer>> prepare(int x, int index, int proposalNum) {
		Prepare prepare = new Prepare();
		prepare.proposalNum = proposalNum;
		prepare.logIdx = index;
		AtomicInteger success = new AtomicInteger(0);
		AtomicInteger noMoreAccepted = new AtomicInteger(0);
		AtomicReference<Pair<Integer, Integer>> maxProposal = new AtomicReference<>(Pair.of(0, x));

		// (5) 一旦收到超过半数接受者的Prepare消息的响应，则判断：如果所有响应中最大的reply.acceptedProposal不等于0，
		// 那么使他的reply.acceptedValue作为提案值，否则使用客户端请求的inputValue作为提案值：另外，
		// 如果超过半数的接受者回复了reply.noMoreAccepted True，那么prepared = true
		Util.concurrencyCall(node.getOthers(), n -> {
			AbstractState state = n.getState();
			if (!(state instanceof Acceptor)) {
				return;
			}
			Acceptor acceptor = (Acceptor) state;
			Promise promise = acceptor.prepare(prepare);
			if (promise != null) {

				// 统计promise成功数
				success.incrementAndGet();

				// 统计noMoreAccepted数目
				if (promise.noMoreAccepted) {
					noMoreAccepted.incrementAndGet();
				}

				// 记录已接收的proposal
				if (promise.acceptedNum > 0) {
					maxProposal.accumulateAndGet(Pair.of(promise.acceptedNum, promise.acceptedVal), (prev, pair) -> {
						if (prev.getKey() < pair.getKey()) {
							return pair;
						}
						return prev;
					});
				}
			}
		});

		// 没过半直接返回
		int majority = node.getOthers().size() / 2;
		if (success.get() <= majority) {
			return null;
		}

		// 过半数的机器的日志都没leader的新则可以不需要prepare了
		if (noMoreAccepted.get() > majority) {
			prepared = true;
		}
		return maxProposal;
	}

	private boolean accept(int val, int index, int proposalNum) {
		Accept accept = new Accept();
		accept.logIdx = index;
		accept.proposalNum = proposalNum;
		accept.proposalVal = val;
		accept.firstUnchosenIndex = firstUnchosenIndex;
		AtomicInteger success = new AtomicInteger(0);

		// (7) 对于收到的每一个接受者的响应都应该进行如下判断：
		// 1.如果reply.n > n 则依据reply.n修改maxRound为最新的值，修改prepared=False,跳转(1)
		// 2.如果reply.firstUnchosenIndex <= lastLogIndex
		// 并且 logs[accepted.firstUnchosenIndex].proposalNum = ∞ ，
		// 则发送Success(index = reply.firstUnchosenIndex, value =
		// logs[reply.firstUnchosenIndex].proposalVal)
		// 用于修复接受者的日志
		Util.concurrencyCall(node.getOthers(), n -> {
			AbstractState state = n.getState();
			if (!(state instanceof Acceptor)) {
				return;
			}
			Acceptor acceptor = (Acceptor) state;
			Accepted accepted = acceptor.accept(accept);

			// 如果acceptor的提案编号大于leader的，则说明leader的数据比较旧，需要重新同步更新。
			if (!accepted.ok) {
				if (accepted.proposalNum > this.maxRound.get()) {
					prepared = false;
					this.maxRound.set(accepted.proposalNum);
				}
				return;
			}
			success.incrementAndGet();

			// 如果Acceptor的firstUnchosenIndex 小于等于 lastLogIndex
			// 并且 logs[accepted.firstUnchosenIndex].proposalNum = ∞ 时，同步Acceptor数据
			if (accepted.firstUnchosenIndex <= lastLogIndex.get()) {

				// 异步循环调用success 修复Acceptor数据。
				pool.execute(() -> {
					int idx = accepted.firstUnchosenIndex;
					while (idx < firstUnchosenIndex) {
						Success succ = new Success();
						succ.logIdx = idx;
						succ.val = n.getLogs().get(idx).getValue();
						idx = acceptor.success(succ);
					}
				});
			}
		});

		int majority = node.getOthers().size() / 2;
		if (success.get() <= majority) {
			log.debug("accept failed: proposal {}", val);
		}
		return success.get() > majority;
	}

	private void success(int val, int index) {

		// 更新日志
		node.getLogs().put(index, Pair.of(CHOSEN, val));

		// 更新firstUnchosenIndex
		int i;
		for (i = firstUnchosenIndex; i <= lastLogIndex.get(); i++) {
			Pair<Integer, Integer> logEntry = node.getLogs().get(i);
			if (logEntry == null || logEntry.getKey() != CHOSEN) {
				break;
			}
		}
		firstUnchosenIndex = i;
	}

	private int proposalNum(int term) {
		return term << 16 | node.getId();
	}

	@Override
	public String toString() {
		return "Leader{" + "maxRound=" + maxRound + ", nextIndex=" + nextIndex + ", prepared=" + prepared
				+ ", firstUnchosenIndex=" + firstUnchosenIndex + '}';
	}
}
