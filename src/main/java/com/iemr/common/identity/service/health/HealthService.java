/*
* AMRIT – Accessible Medical Records via Integrated Technology 
* Integrated EHR (Electronic Health Records) Solution 
*
* Copyright (C) "Piramal Swasthya Management and Research Institute" 
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/

package com.iemr.common.identity.service.health;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.sql.DataSource;
import jakarta.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class HealthService {

    private static final Logger logger = LoggerFactory.getLogger(HealthService.class);
    private static final String STATUS_KEY = "status";
    private static final String DB_HEALTH_CHECK_QUERY = "SELECT 1 as health_check";
    private static final String STATUS_UP = "UP";
    private static final String STATUS_DOWN = "DOWN";
    private static final String ELASTICSEARCH_TYPE = "Elasticsearch";
    private static final int REDIS_TIMEOUT_SECONDS = 3;

    private final DataSource dataSource;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    private final RedisTemplate<String, Object> redisTemplate;
    private final String elasticsearchHost;
    private final int elasticsearchPort;
    private final boolean elasticsearchEnabled;
    private RestClient elasticsearchRestClient;
    private boolean elasticsearchClientReady = false;

    public HealthService(DataSource dataSource,
                        @Autowired(required = false) RedisTemplate<String, Object> redisTemplate,
                        @Value("${elasticsearch.host:localhost}") String elasticsearchHost,
                        @Value("${elasticsearch.port:9200}") int elasticsearchPort,
                        @Value("${elasticsearch.enabled:false}") boolean elasticsearchEnabled) {
        this.dataSource = dataSource;
        this.redisTemplate = redisTemplate;
        this.elasticsearchHost = elasticsearchHost;
        this.elasticsearchPort = elasticsearchPort;
        this.elasticsearchEnabled = elasticsearchEnabled;
    }
    
    @PostConstruct
    public void init() {
        // Initialize Elasticsearch RestClient if enabled
        if (elasticsearchEnabled) {
            initializeElasticsearchClient();
        }
    }
    
    @jakarta.annotation.PreDestroy
    public void cleanup() {
        executorService.shutdownNow();
        if (elasticsearchRestClient != null) {
            try {
                elasticsearchRestClient.close();
            } catch (IOException e) {
                logger.warn("Error closing Elasticsearch client", e);
            }
        }
    }
    
    private void initializeElasticsearchClient() {
        try {
            this.elasticsearchRestClient = RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, "http")
            )
            .setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                    .setConnectTimeout(3000)
                    .setSocketTimeout(3000)
            )
            .build();
            this.elasticsearchClientReady = true;
            logger.info("Elasticsearch RestClient initialized for {}:{}", elasticsearchHost, elasticsearchPort);
        } catch (Exception e) {
            logger.warn("Failed to initialize Elasticsearch RestClient: {}", e.getMessage());
            this.elasticsearchClientReady = false;
        }
    }

    public Map<String, Object> checkHealth() {
        Map<String, Object> healthStatus = new LinkedHashMap<>();
        Map<String, Object> components = new LinkedHashMap<>();
        boolean overallHealth = true;

        Map<String, Object> mysqlStatus = checkMySQLHealth();
        components.put("mysql", mysqlStatus);
        if (!isHealthy(mysqlStatus)) {
            overallHealth = false;
        }

        if (redisTemplate != null) {
            Map<String, Object> redisStatus = checkRedisHealth();
            components.put("redis", redisStatus);
            if (!isHealthy(redisStatus)) {
                overallHealth = false;
            }
        }

        if (elasticsearchEnabled && elasticsearchClientReady) {
            Map<String, Object> elasticsearchStatus = checkElasticsearchHealth();
            components.put("elasticsearch", elasticsearchStatus);
            if (!isHealthy(elasticsearchStatus)) {
                overallHealth = false;
            }
        }

        healthStatus.put(STATUS_KEY, overallHealth ? STATUS_UP : STATUS_DOWN);
        healthStatus.put("timestamp", Instant.now().toString());
        healthStatus.put("components", components);
        logger.info("Health check completed - Overall status: {}", overallHealth ? STATUS_UP : STATUS_DOWN);

        return healthStatus;
    }

    private Map<String, Object> checkMySQLHealth() {
        Map<String, Object> status = new LinkedHashMap<>();
        
        return performHealthCheck("MySQL", status, () -> {
            try {
                try (Connection connection = dataSource.getConnection()) {
                    try (PreparedStatement stmt = connection.prepareStatement(DB_HEALTH_CHECK_QUERY)) {
                        stmt.setQueryTimeout(3);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) == 1) {
                                return new HealthCheckResult(true, null);
                            }
                        }
                    }
                    return new HealthCheckResult(false, "Query validation failed");
                }
            } catch (Exception e) {
                throw new IllegalStateException("MySQL health check failed: " + e.getMessage(), e);
            }
        });
    }
    


    private Map<String, Object> checkRedisHealth() {
        Map<String, Object> status = new LinkedHashMap<>();

        return performHealthCheck("Redis", status, () -> {
            try {
                // Wrap PING in CompletableFuture with timeout
                String pong = CompletableFuture.supplyAsync(() ->
                    redisTemplate.execute((RedisCallback<String>) connection -> connection.ping()),
                    executorService
                ).get(REDIS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                
                if ("PONG".equals(pong)) {
                    return new HealthCheckResult(true, null);
                }
                return new HealthCheckResult(false, "Ping returned unexpected response");
            } catch (TimeoutException e) {
                return new HealthCheckResult(false, "Redis ping timed out after " + REDIS_TIMEOUT_SECONDS + " seconds");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new HealthCheckResult(false, "Redis health check was interrupted");
            } catch (Exception e) {
                throw new IllegalStateException("Redis health check failed", e);
            }
        });
    }

    private Map<String, Object> checkElasticsearchHealth() {
        Map<String, Object> status = new LinkedHashMap<>();

        return performHealthCheck(ELASTICSEARCH_TYPE, status, () -> {
            if (!elasticsearchClientReady || elasticsearchRestClient == null) {
                logger.debug("Elasticsearch RestClient not ready");
                return new HealthCheckResult(false, "Elasticsearch client not ready");
            }
            
            try {
                // Execute a simple cluster health request
                Request request = new Request("GET", "/_cluster/health");
                var response = elasticsearchRestClient.performRequest(request);
                
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    logger.debug("{} health check successful", ELASTICSEARCH_TYPE);
                    return new HealthCheckResult(true, null);
                }
                return new HealthCheckResult(false, "HTTP " + statusCode);
            } catch (java.net.ConnectException e) {
                logger.error("{} connection refused on {}:{}", ELASTICSEARCH_TYPE, elasticsearchHost, elasticsearchPort, e);
                return new HealthCheckResult(false, "Connection refused");
            } catch (java.io.IOException e) {
                logger.error("{} IO error: {}", ELASTICSEARCH_TYPE, e.getMessage(), e);
                return new HealthCheckResult(false, "IO Error: " + e.getMessage());
            } catch (Exception e) {
                logger.error("{} error: {} - {}", ELASTICSEARCH_TYPE, e.getClass().getSimpleName(), e.getMessage(), e);
                return new HealthCheckResult(false, e.getMessage());
            }
        });
    }

    private Map<String, Object> performHealthCheck(String componentName,
                                                    Map<String, Object> status,
                                                    Supplier<HealthCheckResult> checker) {
        long startTime = System.currentTimeMillis();
        
        try {
            HealthCheckResult result = checker.get();
            long responseTime = System.currentTimeMillis() - startTime;
            
            status.put("responseTimeMs", responseTime);

            if (result.isHealthy) {
                logger.debug("{} health check: UP ({}ms)", componentName, responseTime);
                status.put(STATUS_KEY, STATUS_UP);
            } else {
                String safeError = result.error != null ? result.error : "Health check failed";
                logger.warn("{} health check failed: {}", componentName, safeError);
                status.put(STATUS_KEY, STATUS_DOWN);
            }
            
            return status;
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            logger.error("{} health check failed with exception: {}", componentName, e.getMessage(), e);
            
            String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            
            status.put(STATUS_KEY, STATUS_DOWN);
            status.put("responseTimeMs", responseTime);
            
            return status;
        }
    }

    private boolean isHealthy(Map<String, Object> componentStatus) {
        return STATUS_UP.equals(componentStatus.get(STATUS_KEY));
    }

    private static class HealthCheckResult {
        final boolean isHealthy;
        final String error;

        HealthCheckResult(boolean isHealthy, String error) {
            this.isHealthy = isHealthy;
            this.error = error;
        }
    }
}
