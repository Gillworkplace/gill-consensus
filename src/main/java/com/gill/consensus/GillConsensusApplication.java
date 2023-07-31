package com.gill.consensus;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @author gill
 */
@EnableAspectJAutoProxy
@SpringBootApplication
public class GillConsensusApplication {

    public static void main(String[] args) {
        SpringApplication.run(GillConsensusApplication.class, args);
    }

}
