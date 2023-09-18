package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.gill.consensus.raftplus.model.PersistentProperties;
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

	protected int priority = 0;

	protected AtomicInteger committedIdx = new AtomicInteger(0);

	protected AtomicBoolean stable = new AtomicBoolean(false);

	protected final HeartbeatState heartbeatState = new HeartbeatState(0, 0);

	protected final Lock lock = new ReentrantLock();

	/**
	 * 节点组件
	 */
	protected final Schedulers schedulers = new Schedulers();

	protected final ThreadPools threadPools = new ThreadPools();

	protected final MetaDataManager metaDataManager;

	protected RaftConfig config = new RaftConfig();

	@Getter(AccessLevel.NONE)
	protected transient final RaftMachine machine = new RaftMachine(this);

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
		metaDataManager = new MetaDataManager(new EmptyMetaStorage());
		dataStorage = new EmptyDataStorage();
		logManager = new LogManager(new EmptyLogStorage(), config.getLogConfig());
	}

	public Node(MetaStorage metaStorage, DataStorage dataStorage, LogStorage logStorage) {
		ID = RandomUtil.randomInt(100, 200);
		metaDataManager = new MetaDataManager(metaStorage);
		this.dataStorage = dataStorage;
		this.logManager = new LogManager(logStorage, config.getLogConfig());
	}

	public Node(int id) {
		ID = id;
		metaDataManager = new MetaDataManager(new EmptyMetaStorage());
		dataStorage = new EmptyDataStorage();
		logManager = new LogManager(new EmptyLogStorage(), config.getLogConfig());
	}

	public Node(int id, MetaStorage metaStorage, DataStorage dataStorage, LogStorage logStorage) {
		ID = id;
		metaDataManager = new MetaDataManager(metaStorage);
		this.dataStorage = dataStorage;
		this.logManager = new LogManager(logStorage, config.getLogConfig());
	}

	public long getTerm() {
		return this.metaDataManager.getTerm();
	}

	/**
	 * 自增任期
	 * 
	 * @param originTerm
	 *            起始任期
	 * @return originTerm + 1, -1表示选举自己失败
	 */
	public long electSelf(long originTerm) {
		return metaDataManager.increaseTerm(originTerm, ID);
	}

	public int getCommittedIdx() {
		return committedIdx.get();
	}

	public void setCommittedIdx(int committedIdx) {
		this.committedIdx.accumulateAndGet(committedIdx, Math::max);
	}

	public boolean isStable() {
		return stable.get();
	}

	public void stable() {
		stable.compareAndSet(false, true);
	}

	public void unstable() {
		stable.compareAndSet(true, false);
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
		metaDataManager.init();
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
	 * 降级为follower
	 */
	public void stepDown() {
		stepDown(getTerm());
	}

	/**
	 * 降级为follower
	 */
	public void stepDown(long newTerm) {
		stepDown(newTerm, false);
	}

	/**
	 * 降级为follower
	 */
	public void stepDown(long newTerm, boolean sync) {
		if (this.metaDataManager.acceptHigherOrSameTerm(newTerm)) {
			publishEvent(RaftEvent.FORCE_FOLLOWER, new RaftEventParams(Integer.MAX_VALUE, sync));
		}
	}

	private boolean voteFor(long newTerm, int nodeId) {
		if (this.metaDataManager.voteFor(newTerm, nodeId)) {
			refreshLastHeartbeatTimestamp();
			publishEvent(RaftEvent.FORCE_FOLLOWER, new RaftEventParams(Integer.MAX_VALUE, true));
			return true;
		}
		return false;
	}

	/**
	 * 刷新心跳时间
	 */
	private void refreshLastHeartbeatTimestamp() {
		log.debug("node: {} refresh heartbeat timestamp", ID);
		heartbeatState.set(getTerm(), System.currentTimeMillis());
	}

	@Override
	public boolean ready() {
		return machine.isReady();
	}

	private boolean unlatestLog(long lastLogTerm, long lastLogIdx) {
		Pair<Long, Integer> pair = logManager.lastLog();
		return lastLogTerm <= pair.getKey() && (lastLogTerm != pair.getKey() || lastLogIdx < pair.getValue());
	}

	@Override
	public Reply doPreVote(PreVoteParam param) {

		// 成功条件：
		// · 参数中的任期更大，或任期相同但日志索引更大
		// · 至少一次选举超时时间内没有收到领导者心跳
		log.debug("node: {} receives PRE_VOTE, param: {}", ID, param);
		long pTerm = param.getTerm();
		PersistentProperties properties = metaDataManager.getProperties();
		long term = properties.getTerm();

		// 版本太低 丢弃
		if (pTerm < term) {
			log.debug("node: {} discards PRE_VOTE for the old vote's term, client id: {}", ID, param.getNodeId());
			return new Reply(false, term);
		}

		// 日志不够新 丢弃
		if (pTerm == term && unlatestLog(param.getLastLogTerm(), param.getLastLogIdx())) {
			log.debug("node: {} discards PRE_VOTE for the old vote's log-index, client id: {}", ID, param.getNodeId());
			return new Reply(false, term);
		}

		// 该节点还未超时
		Pair<Long, Long> pair = heartbeatState.get();
		if (System.currentTimeMillis() - pair.getValue() < config.getBaseTimeoutInterval()) {
			log.debug("node: {} discards PRE_VOTE, because the node is not timeout, client id: {}", ID,
					param.getNodeId());
			return new Reply(false, term);
		}
		log.info("node: {} accepts PRE_VOTE, client id: {}", ID, param.getNodeId());
		return new Reply(true, term);
	}

	@Override
	public Reply doRequestVote(RequestVoteParam param) {
		lock.lock();
		try {
			log.debug("node: {} receives REQUEST_VOTE, param: {}", ID, param);
			int nodeId = param.getNodeId();
			long pTerm = param.getTerm();

			PersistentProperties properties = metaDataManager.getProperties();
			long cTerm = properties.getTerm();
			Integer cVotedFor = properties.getVotedFor();

			// 版本太低 丢弃
			if (pTerm < cTerm) {
				log.debug("node: {} discards REQUEST_VOTE for the old vote's cTerm, client id: {}", ID, nodeId);
				return new Reply(false, cTerm);
			}

			// 当前任期已投票
			if (pTerm == cTerm && cVotedFor != null && cVotedFor != nodeId) {
				log.debug(
						"node: {} discards REQUEST_VOTE, because node was voted for {} when term was {}, client id: {}",
						ID, cVotedFor, pTerm, nodeId);
				return new Reply(false, pTerm);
			}

			// 日志不够新 丢弃
			if (unlatestLog(param.getLastLogTerm(), param.getLastLogIdx())) {
				log.debug("node: {} discards REQUEST_VOTE for the old vote's log-index, client id: {}", ID, nodeId);
				return new Reply(false, pTerm);
			}

			if (voteFor(pTerm, nodeId)) {
				log.info("node: {} REQUEST_VOTE has voted for {}, term: {}", ID, nodeId, pTerm);
				return new Reply(true, pTerm);
			}
			log.debug("node: {} REQUEST_VOTE vote for term {} id {} failed ", ID, pTerm, nodeId);
			return new Reply(false, pTerm);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AppendLogReply doAppendLogEntries(AppendLogEntriesParam param) {
		lock.lock();
		try {
			long term = getTerm();
			long pTerm = param.getTerm();
			if (term > pTerm) {
				return new AppendLogReply(false, term);
			}
			refreshLastHeartbeatTimestamp();
			stepDown(pTerm, true);
			stable();

			// 没有logs属性的为ping请求
			if (param.getLogs() == null || param.getLogs().isEmpty()) {
				log.trace("node: {} receive heartbeat from {}", ID, param.getNodeId());
				return new AppendLogReply(true, pTerm);
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
			refreshLastHeartbeatTimestamp();
			return new AppendLogReply(true, pTerm);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Reply doReplicateSnapshot(ReplicateSnapshotParam param) {
		lock.lock();
		try {
			log.debug("node: {} replicate snapshot from {}, term is {}, apply{idx={}, term={}}", ID, param.getNodeId(),
					param.getTerm(), param.getApplyIdx(), param.getApplyTerm());
			long pTerm = param.getTerm();
			long term = getTerm();
			if (pTerm < term) {
				return new Reply(false, term);
			}
			refreshLastHeartbeatTimestamp();
			stepDown(pTerm, true);
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
		calcPriority(null);
		this.followers = nodes.stream().filter(node -> this != node).collect(Collectors.toList());
		loadData();
		this.publishEvent(RaftEvent.INIT, new RaftEventParams(getTerm(), true));
	}

	public synchronized void start(List<? extends Node> nodes, Integer priority) {
		this.machine.start();
		this.nodes = new ArrayList<>(nodes);
		calcPriority(priority);
		this.followers = nodes.stream().filter(node -> this != node).collect(Collectors.toList());
		loadData();
		this.publishEvent(RaftEvent.INIT, new RaftEventParams(getTerm(), true));
	}

	private void calcPriority(Integer priority) {
		if (priority != null) {
			this.priority = priority;
			return;
		}
		List<? extends Node> sort = this.nodes.stream().sorted(Comparator.comparingInt((Node n) -> n.getID()))
				.collect(Collectors.toList());
		for (int i = 0; i < sort.size(); i++) {
			if (sort.get(i).getID() == ID) {
				this.priority = i;
			}
		}
	}

	@Override
	public synchronized void stop() {
		this.publishEvent(RaftEvent.STOP, new RaftEventParams(Integer.MAX_VALUE, true));
		this.machine.stop();
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
		sb.append("persistent properties: ").append(metaDataManager.println()).append(System.lineSeparator());
		sb.append("committed idx: ").append(getCommittedIdx()).append(System.lineSeparator());
		sb.append("heartbeat state: ").append(heartbeatState).append(System.lineSeparator());
		sb.append("nodes: ").append(nodes).append(System.lineSeparator());
		sb.append("followers: ").append(followers).append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("THREAD POOL").append(System.lineSeparator());
		sb.append("cluster pool: ")
				.append(Optional.ofNullable(threadPools.getClusterPool()).map(Object::toString).orElse("none"))
				.append(System.lineSeparator());
		sb.append("api pool: ")
				.append(Optional.ofNullable(threadPools.getApiPool()).map(Object::toString).orElse("none"))
				.append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("SCHEDULER").append(System.lineSeparator());
		sb.append("timeout scheduler: ")
				.append(Optional.ofNullable(schedulers.getTimeoutScheduler()).map(Object::toString).orElse("none"))
				.append(System.lineSeparator());
		sb.append("heartbeat scheduler: ")
				.append(Optional.ofNullable(schedulers.getHeartbeatScheduler()).map(Object::toString).orElse("none"))
				.append(System.lineSeparator());
		sb.append("===================").append(System.lineSeparator());
		sb.append("STATE MACHINE").append(System.lineSeparator());
		sb.append(machine.println()).append(System.lineSeparator());
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
		return String.format("id=%s,state=%s,term=%s;", ID, machine.getState(), getTerm());
	}
}
