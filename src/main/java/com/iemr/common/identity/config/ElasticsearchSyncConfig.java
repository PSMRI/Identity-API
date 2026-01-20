package com.iemr.common.identity.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configuration for async processing and scheduling
 */
@Configuration
@EnableAsync
@EnableScheduling
public class ElasticsearchSyncConfig {

    /**
     * Thread pool for Elasticsearch sync operations
     * Configured for long-running background jobs
     */
    @Bean(name = "elasticsearchSyncExecutor")
    public Executor elasticsearchSyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Only 1-2 sync jobs should run at a time to avoid overwhelming DB/ES
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("es-sync-");
        executor.setKeepAliveSeconds(60);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        
        // Handle rejected tasks
        executor.setRejectedExecutionHandler((r, executor1) -> {
            throw new RuntimeException("Elasticsearch sync queue is full. Please wait for current job to complete.");
        });
        
        executor.initialize();
        return executor;
    }
    
    /**
     * General purpose async executor
     */
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }
}