package com.gill.consensus.common;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * RemoteAop
 *
 * @author gill
 * @version 2023/07/31
 **/
@Aspect
@Slf4j
@Component
@Order(2)
public class CostAop {

	private static final int PRINT_COST = 50;

	@Pointcut("@annotation(com.gill.consensus.common.Remote)")
	private void pointcut() {

	}

	@Around("pointcut()")
	public Object around(ProceedingJoinPoint jp) throws Throwable {
		long start = System.currentTimeMillis();
		Object ret = jp.proceed();
		long cost = System.currentTimeMillis() - start;
		if (log.isDebugEnabled()) {
			log.trace("{} cost: {} ms", jp.getSignature().getName(), cost);
		} else if (cost > PRINT_COST) {
			log.info("{} cost: {} ms", jp.getSignature().getName(), cost);
		}
		return ret;
	}
}
