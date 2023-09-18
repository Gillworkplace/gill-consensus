package com.gill.consensus.raftplus.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * Utils
 *
 * @author gill
 * @version 2023/09/13
 **/
@Slf4j
public class Utils {

	public final static String NO_OP = "no_op";

	/**
	 * 耗时计算
	 * 
	 * @param func
	 *            方法
	 * @param funcName
	 *            方法名
	 * @return 返回结果
	 * @param <T>
	 *            类型
	 */
	public static <T> T cost(Supplier<T> func, String funcName) {
		long start = System.currentTimeMillis();
		T ret = func.get();
		long end = System.currentTimeMillis();
		if (end - start > 0) {
			log.debug("{} cost {}ms", funcName, end - start);
		}
		return ret;
	}

	/**
	 * 睡眠
	 * 
	 * @param time
	 *            时间 （毫秒）
	 */
	public static void sleepQuietly(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			log.warn("thread is interrupted, e: {}", e.getMessage());
		}
	}

	/**
	 * 等待线程池关闭
	 * 
	 * @param executorService
	 *            线程池
	 * @param poolName
	 *            线程池名
	 */
	public static void awaitTermination(ExecutorService executorService, String poolName) {
		long start = System.currentTimeMillis();
		try {
			while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
				log.debug("waiting {} termination, duration: {}", poolName, System.currentTimeMillis() - start);
			}
			log.debug("{} is terminated, duration: {}", poolName, System.currentTimeMillis() - start);
		} catch (InterruptedException e) {
			log.warn("{} awaitTermination is interrupted, e: {}", poolName, e);
		}
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
		AtomicInteger cnt = new AtomicInteger(0);
		CompletableFuture<?>[] futures = nodes.stream().map(follower -> CompletableFuture.supplyAsync(() -> {
			R ret = call.apply(follower);
			log.trace("id: {}, call {} {}, ret: {}", id, method, follower, ret);
			return ret;
		}, pool).thenAccept(reply -> {
			if (successFunc.apply(reply)) {
				cnt.incrementAndGet();
			}
		})).toArray(CompletableFuture[]::new);
		try {
			CompletableFuture.allOf(futures).get(500, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			log.error("majority call interrupted: {}", e.toString());
		}
		return cnt.get() >= nodes.size() / 2;
	}
}
