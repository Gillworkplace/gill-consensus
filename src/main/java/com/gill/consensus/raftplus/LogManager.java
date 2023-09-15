package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gill.consensus.raftplus.apis.LogStorage;
import com.gill.consensus.raftplus.config.RaftConfig;
import com.gill.consensus.raftplus.model.LogEntry;
import com.gill.consensus.raftplus.service.PrintService;

import javafx.util.Pair;

/**
 * LogEntries
 *
 * @author gill
 * @version 2023/09/08
 **/
public class LogManager implements PrintService {

	private final LogStorage logStorage;

	private RaftConfig.LogConfig logConfig;

	private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

	private final Lock readLock = new ReentrantReadWriteLock().readLock();

	/**
	 * logs 必须有一个(0, 0, "")的日志
	 */
	private final TreeMap<Integer, LogEntry> logs = new TreeMap<>(Comparator.comparingInt(l -> l));

	{
		logs.put(0, new LogEntry(0, 0, ""));
	}

	public LogManager(LogStorage logStorage, RaftConfig.LogConfig logConfig) {
		this.logStorage = logStorage;
		this.logConfig = logConfig;
	}

	public void setLogConfig(RaftConfig.LogConfig logConfig) {
		this.logConfig = logConfig;
	}

	/**
	 * 初始化加载日志
	 * 
	 * @param applyIdx
	 *            应用日志ID
	 */
	public void init(int applyIdx) {
		List<LogEntry> logEntries = logStorage.loadFromApplyIdx(logConfig.getLoadLen(), applyIdx);
		writeLock.lock();
		try {
			for (LogEntry logEntry : logEntries) {
				logs.put(logEntry.getIndex(), logEntry);
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 最大索引位置
	 *
	 * @return 最大索引位置
	 */
	public Pair<Long, Integer> lastLog() {
		readLock.lock();
		try {
			LogEntry logEntry = logs.lastEntry().getValue();
			return new Pair<>(logEntry.getTerm(), logEntry.getIndex());
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * create
	 * 
	 * @param term
	 *            任期
	 * @param command
	 *            命令
	 * @return 日志
	 */
	public LogEntry createLog(long term, String command) {
		writeLock.lock();
		try {
			int idx = logs.lastEntry().getKey() + 1;
			LogEntry logEntry = new LogEntry(idx, term, command);
			logs.put(idx, logEntry);
			logStorage.write(logEntry);
			return logEntry;
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * put 日志
	 * 
	 * @param logEntry
	 *            日志
	 */
	public void appendLog(LogEntry logEntry) {
		writeLock.lock();
		try {
			logs.put(logEntry.getIndex(), logEntry);
			logStorage.write(logEntry);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 查询日志
	 * 
	 * @param index
	 *            索引
	 * @return 日志
	 */
	public LogEntry getLog(int index) {
		readLock.lock();
		try {
			return logs.get(index);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 获取 [start, end)的日志
	 * 
	 * @param start
	 *            开始索引
	 * @param end
	 *            结束索引
	 * @return 日志s
	 */
	public List<LogEntry> getLogs(int start, int end) {
		readLock.lock();
		try {
			int firstIdx = logs.firstKey();
			if (firstIdx <= start) {
				return new ArrayList<>(logs.subMap(start, end - 1).values());
			}
			List<LogEntry> res = new ArrayList<>(end - start);
			res.addAll(logStorage.read(start, firstIdx - start));
			res.addAll(logs.subMap(firstIdx, end - 1).values());
			return res;
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public String println() {
		return logs.toString();
	}
}
