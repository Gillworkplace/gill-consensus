package com.gill.consensus.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Util
 *
 * @author gill
 * @version 2023/07/31
 **/
public class Util {

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
}
