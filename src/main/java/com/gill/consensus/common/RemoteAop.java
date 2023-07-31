package com.gill.consensus.common;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import cn.hutool.core.util.RandomUtil;

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

		// 模拟延迟
		Thread.sleep(RandomUtil.randomInt(100));
		Object ret = jp.proceed();

        // 模拟延迟
		Thread.sleep(RandomUtil.randomInt(100));
		return ret;
	}
}
