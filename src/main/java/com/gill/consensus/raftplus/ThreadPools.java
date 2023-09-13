package com.gill.consensus.raftplus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.AccessLevel;
import lombok.Getter;

/**
 * NodePool
 *
 * @author gill
 * @version 2023/09/06
 **/
@Getter
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
		clusterPoolLock.lock();
		try {
			if (this.clusterPool != null) {
				this.clusterPool.shutdown();
			}
			this.clusterPool = clusterPool;
		} finally {
			clusterPoolLock.unlock();
		}
	}

	/**
	 * set
	 *
	 * @param apiPool
	 *            线程池
	 */
	public void setApiPool(ExecutorService apiPool) {
		apiPoolLock.lock();
		try {
			if (this.apiPool != null) {
				this.apiPool.shutdown();
			}
			this.apiPool = apiPool;
		} finally {
			apiPoolLock.unlock();
		}

	}
}
