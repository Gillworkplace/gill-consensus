package com.gill.consensus.raftplus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gill.consensus.raftplus.common.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * NodePool
 *
 * @author gill
 * @version 2023/09/06
 **/
@Getter
@Slf4j
public class ThreadPools {

	@Getter(AccessLevel.NONE)
	private final Lock clusterPoolLock = new ReentrantLock();

	private ExecutorService clusterPool;

	@Getter(AccessLevel.NONE)
	private final Lock apiPoolLock = new ReentrantLock();

	private ExecutorService apiPool;

	/**
	 * set
	 * 
	 * @param clusterPool
	 *            线程池
	 */
	public void setClusterPool(ExecutorService clusterPool) {
		ExecutorService tmp = this.clusterPool;
		clusterPoolLock.lock();
		try {
			this.clusterPool = clusterPool;
		} finally {
			clusterPoolLock.unlock();
		}
		if (tmp != null) {
			tmp.shutdown();
			Utils.awaitTermination(tmp, "clusterPool");
		}
	}

	/**
	 * set
	 *
	 * @param apiPool
	 *            线程池
	 */
	public void setApiPool(ExecutorService apiPool) {
		ExecutorService tmp = this.apiPool;
		apiPoolLock.lock();
		try {
			this.apiPool = apiPool;
		} finally {
			apiPoolLock.unlock();
		}
		if (tmp != null) {
			tmp.shutdown();
			Utils.awaitTermination(tmp, "apiPool");
		}
	}
}
