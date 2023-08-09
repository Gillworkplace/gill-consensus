package com.gill.consensus.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * Util
 *
 * @author gill
 * @version 2023/07/31
 **/
@Slf4j
public class Util {

	public static final int CHOSEN = Integer.MAX_VALUE;

	public static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

	private static final AtomicInteger ID = new AtomicInteger(0);

	public static final ThreadPoolExecutor EXECUTOR_ACCEPTOR = new ThreadPoolExecutor(CPU_CORES + 1, CPU_CORES + 1, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "acceptor-pool"));

	public static final ThreadPoolExecutor EXECUTOR_LEARNER = new ThreadPoolExecutor(CPU_CORES + 1, CPU_CORES + 1, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "learner-pool"));

	/**
	 * getId
	 *
	 * @return id
	 */
	public static int getId() {
		return ID.incrementAndGet();
	}

	/**
	 * concurrencyCall
	 * 
	 * @param arr
	 *            arr
	 * @param call
	 *            call
	 * @param <T>
	 *            T
	 */
	public static <T> void concurrencyCall(List<T> arr, Consumer<T> call) {
		CompletableFuture<?>[] cfs = arr.stream().map(ar -> CompletableFuture.runAsync(() -> call.accept(ar)))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(cfs).join();
	}

	public static <T> List<T> getFollowers(List<T> nodes, Predicate<T> predicate) {
		return nodes.stream().filter(predicate).collect(Collectors.toList());
	}

	/**
	 * majorityCall
	 * 
	 * @param nodes
	 *            nodes
	 * @param call
	 *            call
	 * @param successFunc
	 *            successFunc
	 * @param pool
	 *            pool
	 * @param method
	 *            method
	 * @return boolean
	 * @param <T>
	 *            T
	 * @param <R>
	 *            R
	 */
	public static <T, R> boolean majorityCall(List<T> nodes, Function<T, R> call, Function<R, Boolean> successFunc,
			ExecutorService pool, String method) {
		if (nodes.size() == 0) {
			return true;
		}
		final int id = RandomUtil.randomInt();
		AtomicInteger all = new AtomicInteger(nodes.size());
		AtomicInteger majority = new AtomicInteger((nodes.size() + 1) / 2);
		CountDownLatch latch = new CountDownLatch(1);
		for (T follower : nodes) {
			pool.execute(() -> {
				try {
					R ret = call.apply(follower);
					log.trace("id: {}, call {} {}, ret: {}", id, method, follower, ret);
					if (successFunc.apply(ret)) {
						majority.decrementAndGet();
					}
				} catch (Exception e) {
					log.error("call {} exceptionally, e: {}", method, e);
				}
				all.decrementAndGet();
				if (majority.get() <= 0 || all.get() <= 0) {
					latch.countDown();
				}
			});
		}
		try {
			if (!latch.await(500L, TimeUnit.MILLISECONDS)) {
				log.debug("call {} timeout", method);
			}
		} catch (InterruptedException e) {
			log.error("majority call interrupted: {}", e.toString());
		}
		return majority.get() <= 0;
	}
}
