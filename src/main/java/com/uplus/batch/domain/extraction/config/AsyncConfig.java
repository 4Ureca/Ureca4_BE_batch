package com.uplus.batch.domain.extraction.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {
    @Bean(name = "geminiTaskExecutor")
    public Executor geminiTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(15);    
        executor.setMaxPoolSize(30);    
        executor.setQueueCapacity(100);  
        executor.setThreadNamePrefix("GeminiAPI-");
        executor.initialize();
        return executor;
    }
}
