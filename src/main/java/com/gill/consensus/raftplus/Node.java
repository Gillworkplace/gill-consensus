package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.gill.consensus.raftplus.apis.DataStorage;
import com.gill.consensus.raftplus.apis.EmptyDataStorage;
import com.gill.consensus.raftplus.apis.EmptyLogStorage;
import com.gill.consensus.raftplus.apis.EmptyMetaStorage;
import com.gill.consensus.raftplus.apis.LogStorage;
import com.gill.consensus.raftplus.apis.MetaStorage;
import com.gill.consensus.raftplus.config.RaftConfig;
import com.gill.consensus.raftplus.entity.AppendLogEntriesParam;
import com.gill.consensus.raftplus.entity.AppendLogReply;
import com.gill.consensus.raftplus.entity.PreVoteParam;
import com.gill.consensus.raftplus.entity.ReplicateSnapshotParam;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.entity.RequestVoteParam;
import com.gill.consensus.raftplus.machine.RaftEvent;
import com.gill.consensus.raftplus.machine.RaftEventParams;
import com.gill.consensus.raftplus.machine.RaftMachine;
import com.gill.consensus.raftplus.model.LogEntry;
import com.gill.consensus.raftplus.service.ClusterService;
import com.gill.consensus.raftplus.service.InnerNodeService;
import com.gill.consensus.raftplus.service.PrintService;

import cn.hutool.core.util.RandomUtil;
import javafx.util.Pair;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Node
 *
 * @author gill
 * @version 2023/08/02
 **/
@SuppressWarnings("AlibabaServiceOrDaoClassShouldEndWithImpl")
@Slf4j
@Getter
public class Node implements InnerNodeService, ClusterService, PrintService {

	/**
	 * 节点属性
	 */
	protected final int ID;

	protected AtomicInteger committedIdx = new AtomicInteger(0);

	protected final HeartbeatState heartbeatState = new HeartbeatState(0, 0);

	protected final Lock lock = new ReentrantLock();

	/**
	 * 节点组件
	 */
	protected final Schedulers schedulers = new Schedulers();

	protected final ThreadPools threadPools = new ThreadPools();

	protected final PersistentProperties persistentProperties = new PersistentProperties();

	protected RaftConfig config = new RaftConfig();

	@Getter(AccessLevel.NONE)
	protected transient final RaftMachine machine = new RaftMachine(this);

	protected transient final MetaStorage metaStorage;

	protected transient final DataStorage dataStorage;

	protected transient final LogManager logManager;

	protected transient ProposeHelper proposeHelper = new ProposeHelper(threadPools::getApiPool);

	/**
	 * 集群属性
	 */
	protected Node leader = null;

	protected List<InnerNodeService> followers = Collections.emptyList();

	protected List<? extends Node> nodes = Collections.emptyList();

	public Node() {
		ID = RandomUtil.randomInt(100, 200);
		metaStorage = new EmptyMetaStorage();
		dataStorage = new EmptyDataStorage();
		logManager = new LogManager(new EmptyLogStorage(), config.getLogConfig());
	}

	public Node(MetaStorage metaStorage, DataStorage dataStorage, LogStorage logStorage) {
		ID = RandomUtil.randomInt(100, 200);
		this.metaStorage = metaStorage;
		this.dataStorage = dataStorage;
		this.logManager = new LogManager(logStorage, config.getLogConfig());
	}

	public Node(int id) {
		ID = id;
		metaStorage = new EmptyMetaStorage();
		dataStorage = new EmptyDataStorage();
		logManager = new LogManager(new EmptyLogStorage(), config.getLogConfig());
	}

	public Node(int id, MetaStorage metaStorage, DataStorage dataStorage, LogStorage logStorage) {
		ID = id;
		this.metaStorage = metaStorage;
		this.dataStorage = dataStorage;
		this.logManager = new LogManager(logStorage, config.getLogConfig());
	}

	public long getTerm() {
		return this.persistentProperties.getTerm();
	}

	/**
	 * 任期增长
	 *
	 * @param casTerm
	 *            原任期
	 * @param votedFor
	 *            已投服务器
	 * @return 任期
	 */
	public long increaseTerm(long casTerm, int votedFor) {
		lock.lock();
		try {
			long term = persistentProperties.getTerm();
			if (casTerm != term) {
				return -1;
			}
			return setTermAndVotedFor(term + 1, votedFor);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 强制设置最新任期和投票人
	 * 
	 * @param term
	 *            任期
	 * @param votedFor
	 *            投票人
	 * @return term
	 */
	public long setTermAndVotedFor(long term, Integer votedFor) {
		lock.lock();
		try {
			CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null);
			if (term > getTerm()) {
				this.persistentProperties.setTerm(term);
				this.persistentProperties.setVotedFor(votedFor);
				future1 = CompletableFuture.runAsync(this::refreshLastHeartbeatTimestamp);
			}
			CompletableFuture<Void> future2 = CompletableFuture
					.runAsync(() -> metaStorage.write(this.persistentProperties));
			CompletableFuture.allOf(future1, future2);
			return this.persistentProperties.getTerm();
		} finally {
			lock.unlock();
		}
	}

	public int getCommittedIdx() {
		return committedIdx.get();
	}

	public void setCommittedIdx(int committedIdx) {
		this.committedIdx.accumulateAndGet(committedIdx, Math::max);
	}

	private void loadData() {
		lock.lock();
		try {
			// 初始化元数据
			initMeta();

			// 初始化数据
			int applyIdx = initData();

			// 初始化日志
			initLog(applyIdx);

			// 应用未应用的日志
			applyFrom(applyIdx + 1);
		} finally {
			lock.unlock();
		}
	}

	private void initMeta() {
		log.debug("initialize metadata...");
		persistentProperties.set(metaStorage.read());
		log.debug("finish initializing metadata.");
	}

	private int initData() {
		log.debug("loading snapshot...");
		int applyIdx = dataStorage.loadSnapshot();
		log.debug("finish loading snapshot.");
		return applyIdx;
	}

	private void initLog(int applyIdx) {
		log.debug("loading snapshot...");
		logManager.init(applyIdx);
		log.debug("finish loading snapshot...");
	}

	private void applyFrom(int logIdx) {
		Pair<Long, Integer> lastLog = logManager.lastLog();
		if (lastLog == null) {
			return;
		}
		int lastLogIdx = lastLog.getValue();
		log.debug("applying logs from {} to {} ...", logIdx, lastLogIdx);
		for (int i = logIdx; i <= lastLogIdx; i++) {
			LogEntry logEntry = logManager.getLog(i);
			dataStorage.apply(logEntry.getIndex(), logEntry.getCommand());
		}
		log.debug("finish applying logs.");
	}

	/**
	 * 发布事件，透传给状态机
	 * 
	 * @param event
	 *            事件
	 * @param params
	 *            参数
	 */
	public void publishEvent(RaftEvent event, RaftEventParams params) {
		log.debug("node: {} publishes event: {}", ID, event.name());
		this.machine.publishEvent(event, params);
	}

	/**
	 * 刷新心跳时间
	 */
	private void refreshLastHeartbeatTimestamp() {
		heartbeatState.set(getTerm(), System.currentTimeMillis());
	}

	@Override
	public boolean ready() {
		return machine.isReady();
	}

	private boolean invalidateLog(long lastLogTerm, long lastLogIdx) {
		Pair<Long, Integer> pair = logManager.lastLog();
		return lastLogTerm <= pair.getKey() && (lastLogTerm != pair.getKey() || lastLogIdx < pair.getValue());
	}

	@Override
	public Reply doPreVote(PreVoteParam param) {

		// 成功条件：
		// · 参数中的任期更大，或任期相同但日志索引更大
		// · 至少一次选举超时时间内没有收到领导者心跳
		lock.lock();
		try {
			log.debug("node: {} receives PRE_VOTE , param: {}", ID, param);
			long pTerm = param.getTerm();
			long term = persistentProperties.getTerm();

			// 版本太低 丢弃
			if (pTerm < term) {
				log.debug("node: {} discards PRE_VOTE for the old vote's term, client id: {}", ID, param.getNodeId());
				return new Reply(false, term);
			}

			// 日志不够新 丢弃
			if (pTerm == term && invalidateLog(param.getLastLogTerm(), param.getLastLogIdx())) {
				log.debug("node: {} discards PRE_VOTE for the old vote's log-index, client id: {}", ID,
						param.getNodeId());
				return new Reply(false, term);
			}

			// 该节点还未超时
			Pair<Long, Long> pair = heartbeatState.get();
			if (System.currentTimeMillis() - pair.getValue() < config.getTimeoutInterval()) {
				log.debug("node: {} discards PRE_VOTE, because the node is not timeout, client id: {}", ID,
						param.getNodeId());
				return new Reply(false, term);
			}
			log.info("node: {} accepts PRE_VOTE, client id: {}", ID, param.getNodeId());
			return new Reply(true, term);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Reply doRequestVote(RequestVoteParam param) {
		lock.lock();
		try {
			log.debug("node: {} receives REQUEST_VOTE, param: {}", ID, param);
			long pTerm = param.getTerm();
			long term = persistentProperties.getTerm();

			// 版本太低 丢弃
			if (pTerm < term) {
				log.debug("node: {} discards REQUEST_VOTE for the old vote's term, client id: {}", ID,
						param.getNodeId());
				return new Reply(false, term);
			}

			Integer votedFor = persistentProperties.getVotedFor();

			// 当前任期已投票
			if (pTerm == term && votedFor != null && votedFor != param.getNodeId()) {
				log.debug(
						"node: {} discards REQUEST_VOTE, because node was voted for {} when term was {}, client id: {}",
						ID, votedFor, pTerm, param.getNodeId());
				return new Reply(false, pTerm);
			}

			// 日志不够新 丢弃
			if (invalidateLog(param.getLastLogTerm(), param.getLastLogIdx())) {
				log.debug("node: {} discards REQUEST_VOTE for the old vote's log-index, client id: {}", ID,
						param.getNodeId());
				return new Reply(false, pTerm);
			}

			if (pTerm > term) {
				setTermAndVotedFor(pTerm, param.getNodeId());
			}
			this.publishEvent(RaftEvent.FORCE_FOLLOWER, RaftEventParams.EMPTY);
			log.info("node: {} accepts REQUEST_VOTE, client id: {}", ID, param.getNodeId());
			return new Reply(true, pTerm);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AppendLogReply doAppendLogEntries(AppendLogEntriesParam param) {
		long term = getTerm();
		long pTerm = param.getTerm();
		if (term > pTerm) {
			return new AppendLogReply(false, term);
		}
		lock.lock();
		try {
			term = getTerm();
			if (pTerm > term) {
				log.info("node: {} receive higher term {} message from {}", ID, pTerm, param.getNodeId());
				publishEvent(RaftEvent.ACCEPT_LEADER,
						RaftEventParams.builder().term(pTerm).votedFor(param.getNodeId()).build());
			}
			refreshLastHeartbeatTimestamp();

			// 没有logs属性的为ping请求
			if (param.getLogs() == null || param.getLogs().isEmpty()) {
				log.trace("node: {} receive heartbeat from {}", ID, param.getNodeId());
				return new AppendLogReply(true, pTerm);
			}
			if (pTerm > term) {

				// 返回失败等待节点变为follower后重试appendlogs。
				return new AppendLogReply(false, pTerm);
			}
			log.debug("node: {} receive appends log from {}", ID, param.getNodeId());

			// 日志一致性检查
			int committedIdx = getCommittedIdx();

			// 如果当前节点的committedIdx 大于 leader的 committedIdx
			// 说明当前节点的快照版本超前于 leader的版本，但一切以leader为准，因此需要重新同步快照信息
			if (committedIdx > param.getCommitIdx()) {
				return new AppendLogReply(false, pTerm, true);
			}

			Pair<Long, Integer> pair = logManager.lastLog();
			long lastLogTerm = pair.getKey();
			int lastLogIdx = pair.getValue();

			// 同步日志的其实索引不是本节点的下一个索引位置时，返回本节点的最后的日志索引，从该索引开始修复
			if (lastLogIdx < param.getPreLogIdx()) {
				return new AppendLogReply(false, pTerm, false, lastLogIdx);
			}

			// 如果版本不一致，批量从前 repairLength 的索引开始修复
			if (lastLogTerm != param.getPreLogTerm()) {
				return new AppendLogReply(false, pTerm, false, lastLogIdx - config.getRepairLength());
			}

			List<LogEntry> logs = param.getLogs();

			// 记录日志
			logs.forEach(logManager::appendLog);

			// 应用日志
			for (int idx = committedIdx + 1; idx <= param.getCommitIdx(); idx++) {
				LogEntry logEntry = logManager.getLog(idx);
				dataStorage.apply(idx, logEntry.getCommand());
			}

			// 更新committedIdx
			setCommittedIdx(param.getCommitIdx());
			return new AppendLogReply(true, pTerm);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Reply doReplicateSnapshot(ReplicateSnapshotParam param) {
		lock.lock();
		try {
			long pTerm = param.getTerm();
			long term = getTerm();
			if (pTerm < term) {
				return new Reply(false, term);
			}
			long applyLogTerm = param.getApplyTerm();
			int applyIdx = param.getApplyIdx();
			byte[] data = param.getData();
			dataStorage.saveSnapshot(applyLogTerm, applyIdx, data);
			logManager.appendLog(new LogEntry(applyIdx, applyLogTerm, ""));
			setCommittedIdx(applyIdx);
			return new Reply(true, pTerm);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public synchronized void start(List<? extends Node> nodes) {
		this.machine.start();
		this.nodes = new ArrayList<>(nodes);
		this.followers = nodes.stream().filter(node -> this != node).collect(Collectors.toList());
		loadData();
		RaftEventParams params = RaftEventParams.builder().term(getTerm()).build();
		this.publishEvent(RaftEvent.INIT, params);
	}

	@Override
	public synchronized void stop() {
		RaftEventParams params = RaftEventParams.builder().term(getTerm()).build();
		this.publishEvent(RaftEvent.STOP, params);
	}

	@Override
	public synchronized void clear() {

	}

	@Override
	public int propose(String command) {
		if (!ready()) {
			return -1;
		}
		log.debug("node: {} propose {}", ID, command);
		LogEntry logEntry = logManager.createLog(getTerm(), command);
		proposeHelper.propose(logEntry, () -> {
			if (command != null) {
				log.debug("data storage apply {} {}", logEntry.getIndex(), command);
				dataStorage.apply(logEntry.getIndex(), command);
			}
		});
		return logEntry.getIndex();
	}

	@Override
	public String println() {
		StringBuilder sb = new StringBuilder();
		sb.append("===================").append(System.lineSeparator());
		sb.append("node id: ").append(ID).append("\t").append(machine.getState().name()).append(System.lineSeparator());
		sb.append("persistent properties: ").append(persistentProperties).append(System.lineSeparator());
		sb.append("committed idx: ").append(getCommittedIdx()).append(System.lineSeparator());
		sb.append("heartbeat state: ").append(heartbeatState).append(System.lineSeparator());
		sb.append("nodes: ").append(nodes).append(System.lineSeparator());
		sb.append("followers: ").append(followers).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("CONFIG").append(System.lineSeparator());
		sb.append(config.toString()).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("PROPOSE HELPER").append(System.lineSeparator());
		sb.append(proposeHelper.println()).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("LOG MANAGER").append(System.lineSeparator());
		sb.append(logManager.println()).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("DATA STORAGE").append(System.lineSeparator());
		sb.append(dataStorage.println()).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		return sb.toString();
	}

	@Override
	public String toString() {
		return String.format("node id: %s, state: %s", ID, machine.getState());
	}
}
