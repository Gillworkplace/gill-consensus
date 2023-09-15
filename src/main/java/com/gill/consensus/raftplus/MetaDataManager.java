package com.gill.consensus.raftplus;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gill.consensus.raftplus.apis.MetaStorage;
import com.gill.consensus.raftplus.model.PersistentProperties;

import com.gill.consensus.raftplus.service.PrintService;
import lombok.extern.slf4j.Slf4j;

/**
 * PersistentProperties
 *
 * @author gill
 * @version 2023/09/11
 **/
@Slf4j
public class MetaDataManager implements PrintService {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final Lock readLock = lock.readLock();

	private final Lock writeLock = lock.writeLock();

	private final MetaStorage metaStorage;

	private final PersistentProperties properties = new PersistentProperties();

	public MetaDataManager(MetaStorage metaStorage) {
		this.metaStorage = metaStorage;
	}

	/**
	 * 初始化数据
	 */
	public void init() {
		properties.set(metaStorage.read());
	}

	/**
	 * 获取term
	 * 
	 * @return term
	 */
	public long getTerm() {
		readLock.lock();
		try {
			return properties.getTerm();
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 获取term
	 *
	 * @return term
	 */
	public PersistentProperties getProperties() {
		readLock.lock();
		try {
			PersistentProperties ret = new PersistentProperties();
			ret.set(properties);
			return ret;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 任期增长
	 *
	 * @param originTerm
	 *            原任期
	 * @param votedFor
	 *            已投服务器
	 * @return 任期 -1 为失败
	 */
	public long increaseTerm(long originTerm, int votedFor) {
		writeLock.lock();
		try {
			if(originTerm == properties.getTerm() && voteFor(originTerm + 1, votedFor)) {
				return originTerm + 1;
			}
			return -1;
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 投票
	 * 
	 * @param term
	 *            任期
	 * @param votedFor
	 *            候选人
	 * @return 是否成功投票
	 */
	public boolean voteFor(long term, int votedFor) {
		writeLock.lock();
		try {
			if (term > properties.getTerm()) {
				properties.setTerm(term);
				properties.setVotedFor(votedFor);
				return true;
			}
			return term == properties.getTerm()
					&& (properties.getVotedFor() == null || Objects.equals(votedFor, properties.getVotedFor()));
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 接受了更高任期的消息
	 *
	 * @param newTerm 任期
	 */
	public boolean acceptHigherOrSameTerm(long newTerm) {
		writeLock.lock();
		try {
			if (newTerm > properties.getTerm()) {
				properties.setTerm(newTerm);
				properties.setVotedFor(null);
				return true;
			}
			return newTerm == properties.getTerm();
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public String println() {
		return properties.toString();
	}
}
