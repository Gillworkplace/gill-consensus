package com.gill.consensus.common;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
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

	private static final int SLOW_COST = 50;

	@Pointcut("@annotation(com.gill.consensus.common.Remote) || @annotation(com.gill.consensus.common.Cost)")
	private void pointcut() {

	}

	@Around("pointcut()")
	public Object around(ProceedingJoinPoint jp) throws Throwable {
		long start = System.currentTimeMillis();
		Object ret = jp.proceed();
		long cost = System.currentTimeMillis() - start;

		int threshold = 5;
		Signature signature = jp.getSignature();
		if(signature instanceof MethodSignature) {
			MethodSignature methodSignature = (MethodSignature)signature;
			Method method = methodSignature.getMethod();
			Cost costAnno = method.getAnnotation(Cost.class);
			if(costAnno != null) {
				threshold = costAnno.threshold();
			}
		}

		if (log.isDebugEnabled() && cost > threshold) {
			log.debug("{} cost: {} ms", signature.getName(), cost);
		} else if (cost > SLOW_COST) {
			log.info("{} cost: {} ms", signature.getName(), cost);
		} else {
			log.trace("{} cost: {} ms", signature.getName(), cost);
		}
		return ret;
	}
}
