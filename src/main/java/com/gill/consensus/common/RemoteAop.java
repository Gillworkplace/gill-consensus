package com.gill.consensus.common;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import cn.hutool.core.util.RandomUtil;

import java.lang.reflect.Method;

/**
 * RemoteAop
 *
 * @author gill
 * @version 2023/07/31
 **/
@Aspect
@Slf4j
@Component
@Order(1)
public class RemoteAop {

	@Pointcut("@annotation(com.gill.consensus.common.Remote)")
	private void pointcut() {

	}

	@Around("pointcut()")
	public Object around(ProceedingJoinPoint jp) throws Throwable {

		int deplay = 50;
		Signature signature = jp.getSignature();
		if(signature instanceof MethodSignature) {
			MethodSignature methodSignature = (MethodSignature)signature;
			Method method = methodSignature.getMethod();
			Remote remoteAnno = method.getAnnotation(Remote.class);
			if(remoteAnno != null) {
				deplay = remoteAnno.delay();
			}
		}

		// 模拟延迟
		Thread.sleep(RandomUtil.randomInt(deplay));
		Object ret = jp.proceed();

        // 模拟延迟
		Thread.sleep(RandomUtil.randomInt(deplay));
		return ret;
	}
}
