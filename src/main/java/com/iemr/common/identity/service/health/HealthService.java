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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
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
    private static final String DB_VERSION_QUERY = "SELECT VERSION()";
    private static final String STATUS_UP = "UP";
    private static final String STATUS_DOWN = "DOWN";
    private static final String STATUS_DEGRADED = "DEGRADED";
    private static final String UNKNOWN_VALUE = "unknown";
    private static final String ELASTICSEARCH_TYPE = "Elasticsearch";
    private static final int REDIS_TIMEOUT_SECONDS = 3;
    
    // Severity levels and keys
    private static final String SEVERITY_KEY = "severity";
    private static final String SEVERITY_OK = "OK";
    private static final String SEVERITY_WARNING = "WARNING";
    private static final String SEVERITY_CRITICAL = "CRITICAL";
    private static final long RESPONSE_TIME_THRESHOLD_MS = 2000;
    private static final long ADVANCED_CHECKS_THROTTLE_SECONDS = 30; 
    
    // Response keys
    private static final String ERROR_KEY = "error";
    private static final String MESSAGE_KEY = "message";
    private static final String RESPONSE_TIME_KEY = "responseTimeMs";
    
    // Diagnostic event codes for concise logging
    private static final String DIAGNOSTIC_LOCK_WAIT = "MYSQL_LOCK_WAIT";
    private static final String DIAGNOSTIC_DEADLOCK = "MYSQL_DEADLOCK";
    private static final String DIAGNOSTIC_SLOW_QUERIES = "MYSQL_SLOW_QUERIES";
    private static final String DIAGNOSTIC_POOL_EXHAUSTED = "MYSQL_POOL_EXHAUSTED";
    private static final String DIAGNOSTIC_LOG_TEMPLATE = "Diagnostic: {}";

    private final DataSource dataSource;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    private final RedisTemplate<String, Object> redisTemplate;
    private final String dbUrl;
    private final String redisHost;
    private final int redisPort;
    private final String elasticsearchHost;
    private final int elasticsearchPort;
    private final boolean elasticsearchEnabled;
    private RestClient elasticsearchRestClient;
    private boolean elasticsearchClientReady = false;
    
    // Advanced checks throttling (thread-safe)
    private volatile long lastAdvancedCheckTime = 0;
    private volatile AdvancedCheckResult cachedAdvancedCheckResult = null;
    private final ReentrantReadWriteLock advancedCheckLock = new ReentrantReadWriteLock();
    
    // Deadlock check resilience - disable after first permission error
    private volatile boolean deadlockCheckDisabled = false;

    public HealthService(DataSource dataSource,
                        @Autowired(required = false) RedisTemplate<String, Object> redisTemplate,
                        @Value("${spring.datasource.url:unknown}") String dbUrl,
                        @Value("${spring.data.redis.host:localhost}") String redisHost,
                        @Value("${spring.data.redis.port:6379}") int redisPort,
                        @Value("${elasticsearch.host:localhost}") String elasticsearchHost,
                        @Value("${elasticsearch.port:9200}") int elasticsearchPort,
                        @Value("${elasticsearch.enabled:false}") boolean elasticsearchEnabled) {
        this.dataSource = dataSource;
        this.redisTemplate = redisTemplate;
        this.dbUrl = dbUrl;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
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

    public Map<String, Object> checkHealth(boolean includeDetails) {
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

    public Map<String, Object> checkHealth() {
        return checkHealth(true);
    }

    private Map<String, Object> checkMySQLHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", "MySQL");

        return performHealthCheck("MySQL", details, () -> {
            try {
                try (Connection connection = dataSource.getConnection()) {
                    if (connection.isValid(2)) {
                        try (PreparedStatement stmt = connection.prepareStatement(DB_HEALTH_CHECK_QUERY)) {
                            stmt.setQueryTimeout(3);
                            try (ResultSet rs = stmt.executeQuery()) {
                                if (rs.next() && rs.getInt(1) == 1) {
                                    // Basic check passed - run advanced checks with throttling
                                    boolean isDegraded = performAdvancedMySQLChecksWithThrottle(connection);
                                    return new HealthCheckResult(true, null, isDegraded);
                                }
                            }
                        }
                    }
                    return new HealthCheckResult(false, "Connection validation failed", false);
                }
            } catch (Exception e) {
                throw new IllegalStateException("MySQL connection failed: " + e.getMessage(), e);
            }
        });
    }
    


    private Map<String, Object> checkRedisHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", "Redis");

        return performHealthCheck("Redis", details, () -> {
            try {
                // Run Redis PING synchronously - avoid nested CompletableFuture on same executor
                String pong = redisTemplate.execute((RedisCallback<String>) connection -> connection.ping());
                
                if ("PONG".equals(pong)) {
                    return new HealthCheckResult(true, null, false);
                }
                return new HealthCheckResult(false, "Ping returned unexpected response", false);
            } catch (Exception e) {
                throw new IllegalStateException("Redis health check failed", e);
            }
        });
    }

    private Map<String, Object> checkElasticsearchHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", ELASTICSEARCH_TYPE);

        return performHealthCheck(ELASTICSEARCH_TYPE, details, () -> {
            if (!elasticsearchClientReady || elasticsearchRestClient == null) {
                logger.debug("Elasticsearch RestClient not ready");
                return new HealthCheckResult(false, "Elasticsearch client not ready", false);
            }
            
            try {
                // Execute a simple cluster health request
                Request request = new Request("GET", "/_cluster/health");
                var response = elasticsearchRestClient.performRequest(request);
                
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    logger.debug("{} health check successful", ELASTICSEARCH_TYPE);
                    return new HealthCheckResult(true, null, false);
                }
                return new HealthCheckResult(false, "HTTP " + statusCode, false);
            } catch (java.net.ConnectException e) {
                logger.error("{} connection refused on {}:{}", ELASTICSEARCH_TYPE, elasticsearchHost, elasticsearchPort, e);
                return new HealthCheckResult(false, "Connection refused", false);
            } catch (java.io.IOException e) {
                logger.error("{} IO error: {}", ELASTICSEARCH_TYPE, e.getMessage(), e);
                return new HealthCheckResult(false, "IO Error: " + e.getMessage(), false);
            } catch (Exception e) {
                logger.error("{} error: {} - {}", ELASTICSEARCH_TYPE, e.getClass().getSimpleName(), e.getMessage(), e);
                return new HealthCheckResult(false, e.getMessage(), false);
            }
        });
    }

    private Map<String, Object> performHealthCheck(String componentName,
                                                    Map<String, Object> details,
                                                    Supplier<HealthCheckResult> checker) {
        Map<String, Object> status = new LinkedHashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            HealthCheckResult result = checker.get();
            long responseTime = System.currentTimeMillis() - startTime;
            
            details.put(RESPONSE_TIME_KEY, responseTime);

            if (result.isHealthy) {
                buildHealthyStatus(status, details, componentName, responseTime, result);
            } else {
                buildUnhealthyStatus(status, details, componentName, result);
            }
            
            status.put("details", details);
            return status;
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            return buildExceptionStatus(status, details, componentName, e, responseTime);
        }
    }

    private void buildHealthyStatus(Map<String, Object> status, Map<String, Object> details,
                                    String componentName, long responseTime, HealthCheckResult result) {
        logger.debug("{} health check: UP ({}ms)", componentName, responseTime);
        
        // Determine status based on health, response time, and degradation flags
        String statusValue = result.isDegraded ? STATUS_DEGRADED : STATUS_UP;
        status.put(STATUS_KEY, statusValue);
        
        String severity = determineSeverity(result.isHealthy, responseTime, result.isDegraded);
        status.put(SEVERITY_KEY, severity);
        
        // Include message if there's an error (informational when healthy)
        if (result.error != null) {
            status.put(MESSAGE_KEY, result.error);
        }
    }

    private void buildUnhealthyStatus(Map<String, Object> status, Map<String, Object> details,
                                      String componentName, HealthCheckResult result) {
        String safeError = result.error != null ? result.error : "Health check failed";
        logger.warn("{} health check failed: {}", componentName, safeError);
        status.put(STATUS_KEY, STATUS_DOWN);
        status.put(SEVERITY_KEY, SEVERITY_CRITICAL);
        details.put(ERROR_KEY, safeError);
        details.put("errorType", "CheckFailed");
    }

    private Map<String, Object> buildExceptionStatus(Map<String, Object> status, Map<String, Object> details,
                                                      String componentName, Exception e, long responseTime) {
        logger.error("{} health check failed with exception: {}", componentName, e.getMessage(), e);
        
        String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
        
        status.put(STATUS_KEY, STATUS_DOWN);
        status.put(SEVERITY_KEY, SEVERITY_CRITICAL);
        details.put(RESPONSE_TIME_KEY, responseTime);
        details.put(ERROR_KEY, errorMessage != null ? errorMessage : "Health check failed");
        status.put("details", details);
        
        return status;
    }
    private String determineSeverity(boolean isHealthy, long responseTimeMs, boolean isDegraded) {
        if (!isHealthy) {
            return SEVERITY_CRITICAL;
        }
        
        if (isDegraded) {
            return SEVERITY_WARNING;
        }
        
        if (responseTimeMs > RESPONSE_TIME_THRESHOLD_MS) {
            return SEVERITY_WARNING;
        }
        
        return SEVERITY_OK;
    }

    private boolean isHealthy(Map<String, Object> componentStatus) {
        return STATUS_UP.equals(componentStatus.get(STATUS_KEY));
    }

    private String getRedisVersion() {
        try {
            Properties info = redisTemplate.execute((RedisCallback<Properties>) connection ->
                connection.serverCommands().info("server")
            );
            if (info != null && info.containsKey("redis_version")) {
                return info.getProperty("redis_version");
            }
        } catch (Exception e) {
            logger.debug("Could not retrieve Redis version", e);
        }
        return null;
    }

    
    private boolean performAdvancedMySQLChecksWithThrottle(Connection connection) {
        long currentTime = System.currentTimeMillis();
        
        advancedCheckLock.readLock().lock();
        try {
            if (cachedAdvancedCheckResult != null && 
                (currentTime - lastAdvancedCheckTime) < ADVANCED_CHECKS_THROTTLE_SECONDS * 1000) {
                return cachedAdvancedCheckResult.isDegraded;
            }
        } finally {
            advancedCheckLock.readLock().unlock();
        }
        
        advancedCheckLock.writeLock().lock();
        try {
            currentTime = System.currentTimeMillis();
            // Double-check after acquiring write lock
            if (cachedAdvancedCheckResult != null && 
                (currentTime - lastAdvancedCheckTime) < ADVANCED_CHECKS_THROTTLE_SECONDS * 1000) {
                return cachedAdvancedCheckResult.isDegraded;
            }
            
            AdvancedCheckResult result = performAdvancedMySQLChecks(connection);
            
            // Cache the result
            lastAdvancedCheckTime = currentTime;
            cachedAdvancedCheckResult = result;
            
            return result.isDegraded;
        } finally {
            advancedCheckLock.writeLock().unlock();
        }
    }

    private AdvancedCheckResult performAdvancedMySQLChecks(Connection connection) {
        try {
            boolean hasIssues = false;
            
            if (hasLockWaits(connection)) {
                logger.warn(DIAGNOSTIC_LOG_TEMPLATE, DIAGNOSTIC_LOCK_WAIT);
                hasIssues = true;
            }
            
            if (hasDeadlocks(connection)) {
                logger.warn(DIAGNOSTIC_LOG_TEMPLATE, DIAGNOSTIC_DEADLOCK);
                hasIssues = true;
            }
            
            if (hasSlowQueries(connection)) {
                logger.warn(DIAGNOSTIC_LOG_TEMPLATE, DIAGNOSTIC_SLOW_QUERIES);
                hasIssues = true;
            }
            
            if (hasConnectionPoolExhaustion()) {
                logger.warn(DIAGNOSTIC_LOG_TEMPLATE, DIAGNOSTIC_POOL_EXHAUSTED);
                hasIssues = true;
            }
            
            return new AdvancedCheckResult(hasIssues);
        } catch (Exception e) {
            logger.debug("Advanced MySQL checks encountered exception, marking degraded");
            return new AdvancedCheckResult(true);
        }
    }

    private boolean hasLockWaits(Connection connection) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST " +
                "WHERE (state = 'Waiting for table metadata lock' " +
                "   OR state = 'Waiting for row lock' " +
                "   OR state = 'Waiting for lock') " +
                "AND user = USER()")) {
            stmt.setQueryTimeout(2);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int lockCount = rs.getInt(1);
                    return lockCount > 0;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check for lock waits");
        }
        return false;
    }

    private boolean hasDeadlocks(Connection connection) {
        // Skip deadlock check if already disabled due to permissions
        if (deadlockCheckDisabled) {
            return false;
        }
        
        try (PreparedStatement stmt = connection.prepareStatement("SHOW ENGINE INNODB STATUS")) {
            stmt.setQueryTimeout(2);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String innodbStatus = rs.getString(3);
                    return innodbStatus != null && innodbStatus.contains("LATEST DETECTED DEADLOCK");
                }
            }
        } catch (java.sql.SQLException e) {
            // Check if this is a permission error
            if (e.getMessage() != null && 
                (e.getMessage().contains("Access denied") || 
                 e.getMessage().contains("permission"))) {
                // Disable this check permanently after first permission error
                deadlockCheckDisabled = true;
                logger.warn("Deadlock check disabled: Insufficient privileges");
            } else {
                logger.debug("Could not check for deadlocks");
            }
        } catch (Exception e) {
            logger.debug("Could not check for deadlocks");
        }
        return false;
    }

    private boolean hasSlowQueries(Connection connection) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST " +
                "WHERE command != 'Sleep' AND time > ? AND user NOT IN ('event_scheduler', 'system user')")) {
            stmt.setQueryTimeout(2);
            stmt.setInt(1, 10); // Queries running longer than 10 seconds
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int slowQueryCount = rs.getInt(1);
                    return slowQueryCount > 3; // Alert if more than 3 slow queries
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check for slow queries");
        }
        return false;
    }

    private boolean hasConnectionPoolExhaustion() {
        // Use HikariCP metrics if available
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            try {
                HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
                
                if (poolMXBean != null) {
                    int activeConnections = poolMXBean.getActiveConnections();
                    int maxPoolSize = hikariDataSource.getMaximumPoolSize();
                    
                    // Alert if > 80% of pool is exhausted
                    int threshold = (int) (maxPoolSize * 0.8);
                    return activeConnections > threshold;
                }
            } catch (Exception e) {
                logger.debug("Could not retrieve HikariCP pool metrics");
            }
        }
        
        // Fallback: try to get pool metrics via JMX if HikariCP is not directly available
        return checkPoolMetricsViaJMX();
    }

    private boolean checkPoolMetricsViaJMX() {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.zaxxer.hikari:type=Pool (*)");
            var mBeans = mBeanServer.queryMBeans(objectName, null);
            
            for (var mBean : mBeans) {
                if (evaluatePoolMetrics(mBeanServer, mBean.getObjectName())) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not access HikariCP pool metrics via JMX");
        }
        
        // No pool metrics available - disable this check
        logger.debug("Pool exhaustion check disabled: HikariCP metrics unavailable");
        return false;
    }

    private boolean evaluatePoolMetrics(MBeanServer mBeanServer, ObjectName objectName) {
        try {
            Integer activeConnections = (Integer) mBeanServer.getAttribute(objectName, "ActiveConnections");
            Integer maximumPoolSize = (Integer) mBeanServer.getAttribute(objectName, "MaximumPoolSize");
            
            if (activeConnections != null && maximumPoolSize != null) {
                int threshold = (int) (maximumPoolSize * 0.8);
                return activeConnections > threshold;
            }
        } catch (Exception e) {
            // Continue to next MBean
        }
        return false;
    }

    private static class AdvancedCheckResult {
        final boolean isDegraded;

        AdvancedCheckResult(boolean isDegraded) {
            this.isDegraded = isDegraded;
        }
    }

    private static class HealthCheckResult {
        final boolean isHealthy;
        final String error;
        final boolean isDegraded;

        HealthCheckResult(boolean isHealthy, String error, boolean isDegraded) {
            this.isHealthy = isHealthy;
            this.error = error;
            this.isDegraded = isDegraded;
        }
    }
}
