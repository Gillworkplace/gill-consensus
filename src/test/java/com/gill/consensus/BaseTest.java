package com.gill.consensus;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

/**
 * BaseTest
 *
 * @author gill
 * @version 2023/08/01
 **/
public abstract class BaseTest {

	@Autowired
	private ApplicationContext context;

	protected <T> T newTarget(Class<T> clazz) {
		return context.getBean(clazz);
	}

	protected <T> T newTarget(String beanName, Class<T> clazz) {
		return context.getBean(beanName, clazz);
	}

	protected <T> List<T> newList(int size, Class<T> clazz) {
		List<T> arr = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			arr.add(newTarget(clazz));
		}
		return arr;
	}

	protected <T> List<T> newList(int size, String beanName, Class<T> clazz) {
		List<T> arr = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			arr.add(newTarget(beanName, clazz));
		}
		return arr;
	}

	protected static <T> void print(List<T> targets) {
		System.out.println();
		for (T target : targets) {
			System.out.printf("%s%n", target);
			printSplit();
		}
		System.out.println();
	}

	protected static void printSplit() {
		System.out.println("=========");
	}

	protected static void sleep(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
