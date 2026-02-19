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
    private static final String DB_VERSION_QUERY = "SELECT VERSION()";
    private static final String STATUS_UP = "UP";
    private static final String STATUS_DOWN = "DOWN";
    private static final String UNKNOWN_VALUE = "unknown";
    private static final String ELASTICSEARCH_TYPE = "Elasticsearch";
    private static final int REDIS_TIMEOUT_SECONDS = 3;

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

        Map<String, Object> mysqlStatus = checkMySQLHealth(includeDetails);
        components.put("mysql", mysqlStatus);
        if (!isHealthy(mysqlStatus)) {
            overallHealth = false;
        }

        if (redisTemplate != null) {
            Map<String, Object> redisStatus = checkRedisHealth(includeDetails);
            components.put("redis", redisStatus);
            if (!isHealthy(redisStatus)) {
                overallHealth = false;
            }
        }

        if (elasticsearchEnabled && elasticsearchClientReady) {
            Map<String, Object> elasticsearchStatus = checkElasticsearchHealth(includeDetails);
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

    private Map<String, Object> checkMySQLHealth(boolean includeDetails) {
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
                                    return new HealthCheckResult(true, null, null);
                                }
                            }
                        }
                    }
                    return new HealthCheckResult(false, null, "Connection validation failed");
                }
            } catch (Exception e) {
                throw new IllegalStateException("MySQL connection failed: " + e.getMessage(), e);
            }
        }, includeDetails);
    }
    
    private void addAdvancedMySQLMetrics(Connection connection, Map<String, Object> details) {
        try {
            // Only add advanced metrics for MySQL, not for H2 or other databases
            if (!isMySQLDatabase(connection)) {
                logger.debug("Advanced metrics only supported for MySQL, skipping for this database");
                return;
            }
            
            Map<String, Object> metrics = new LinkedHashMap<>();
            
            // Get server status variables (uptime, connections, etc.)
            Map<String, String> statusVars = getMySQLStatusVariables(connection);
            
            if (!statusVars.isEmpty()) {
                // Connections
                Map<String, Object> connections = new LinkedHashMap<>();
                String threads = statusVars.get("Threads_connected");
                String maxConnections = statusVars.get("max_connections");
                if (threads != null && maxConnections != null) {
                    connections.put("active", Integer.parseInt(threads));
                    connections.put("max", Integer.parseInt(maxConnections));
                    try {
                        int active = Integer.parseInt(threads);
                        int max = Integer.parseInt(maxConnections);
                        connections.put("usage_percent", (active * 100) / max);
                    } catch (NumberFormatException e) {
                        logger.debug("Could not calculate connection usage percent");
                    }
                }
                if (!connections.isEmpty()) {
                    metrics.put("connections", connections);
                }
                
                // Uptime
                String uptime = statusVars.get("Uptime");
                if (uptime != null) {
                    try {
                        long uptimeSeconds = Long.parseLong(uptime);
                        metrics.put("uptime_seconds", uptimeSeconds);
                        metrics.put("uptime_hours", uptimeSeconds / 3600);
                    } catch (NumberFormatException e) {
                        logger.debug("Could not parse uptime");
                    }
                }
                
                // Slow queries
                String slowQueries = statusVars.get("Slow_queries");
                if (slowQueries != null) {
                    metrics.put("slow_queries", Integer.parseInt(slowQueries));
                }
                
                // Questions (total queries)
                String questions = statusVars.get("Questions");
                if (questions != null) {
                    metrics.put("total_queries", Long.parseLong(questions));
                }
            }
            
            // Database size
            Map<String, Object> database = new LinkedHashMap<>();
            try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT table_schema, ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb " +
                "FROM information_schema.tables WHERE table_schema = database() GROUP BY table_schema")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        double sizeMb = rs.getDouble("size_mb");
                        database.put("size_mb", sizeMb);
                    }
                }
            } catch (Exception e) {
                logger.debug("Could not retrieve database size", e);
            }
            
            // Table count
            try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = database()")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        database.put("tables_count", rs.getInt("table_count"));
                    }
                }
            } catch (Exception e) {
                logger.debug("Could not retrieve table count", e);
            }
            
            if (!database.isEmpty()) {
                metrics.put("database", database);
            }
            
            // Add deep health checks (locks, stuck processes, deadlocks)
            addDeepMySQLHealthChecks(connection, metrics);
            
            if (!metrics.isEmpty()) {
                details.put("metrics", metrics);
            }
            
        } catch (Exception e) {
            logger.debug("Error retrieving advanced MySQL metrics", e);
        }
    }
    
    private boolean isMySQLDatabase(Connection connection) {
        try {
            String databaseProductName = connection.getMetaData().getDatabaseProductName();
            return databaseProductName != null && databaseProductName.toLowerCase().contains("mysql");
        } catch (Exception e) {
            logger.debug("Could not determine database type", e);
            return false;
        }
    }
    
    private void addDeepMySQLHealthChecks(Connection connection, Map<String, Object> metrics) {
        // Check for table locks
        checkTableLocks(connection, metrics);
        
        // Check for stuck/long-running queries
        checkStuckProcesses(connection, metrics);
        
        // Check for InnoDB deadlocks
        checkInnoDBStatus(connection, metrics);
    }
    
    private void checkTableLocks(Connection connection, Map<String, Object> metrics) {
        try (PreparedStatement stmt = connection.prepareStatement(
            "SELECT OBJECT_SCHEMA, OBJECT_NAME, COUNT(*) as lock_count " +
            "FROM INFORMATION_SCHEMA.METADATA_LOCKS " +
            "WHERE OBJECT_SCHEMA != 'mysql' AND OBJECT_SCHEMA != 'information_schema' " +
            "GROUP BY OBJECT_SCHEMA, OBJECT_NAME")) {
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, Object> locks = new LinkedHashMap<>();
                int totalLocks = 0;
                java.util.List<String> lockedTables = new java.util.ArrayList<>();
                
                while (rs.next()) {
                    totalLocks += rs.getInt("lock_count");
                    String tableName = rs.getString("OBJECT_SCHEMA") + "." + rs.getString("OBJECT_NAME");
                    lockedTables.add(tableName);
                }
                
                if (totalLocks > 0) {
                    locks.put("is_locked", true);
                    locks.put("locked_tables_count", lockedTables.size());
                    locks.put("locked_tables", lockedTables);
                    locks.put("total_locks", totalLocks);
                    locks.put("severity", totalLocks > 5 ? "CRITICAL" : "WARNING");
                    metrics.put("table_locks", locks);
                    logger.warn("MySQL: {} table locks detected on {} tables", totalLocks, lockedTables.size());
                } else {
                    locks.put("is_locked", false);
                    locks.put("locked_tables_count", 0);
                    metrics.put("table_locks", locks);
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check table locks (might not support METADATA_LOCKS)", e);
        }
    }
    
    private void checkStuckProcesses(Connection connection, Map<String, Object> metrics) {
        try (PreparedStatement stmt = connection.prepareStatement(
            "SELECT ID, USER, HOST, DB, TIME, COMMAND, STATE, INFO " +
            "FROM INFORMATION_SCHEMA.PROCESSLIST " +
            "WHERE COMMAND != 'Sleep' AND TIME > 300")) {  // Processes running > 5 minutes
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, Object> processes = new LinkedHashMap<>();
                java.util.List<Map<String, Object>> stuckQueries = new java.util.ArrayList<>();
                int totalRunning = 0;
                long longestQuerySeconds = 0;
                
                while (rs.next()) {
                    long queryTime = rs.getLong("TIME");
                    longestQuerySeconds = Math.max(longestQuerySeconds, queryTime);
                    
                    Map<String, Object> query = new LinkedHashMap<>();
                    query.put("id", rs.getInt("ID"));
                    query.put("user", rs.getString("USER"));
                    query.put("command", rs.getString("COMMAND"));
                    query.put("time_seconds", queryTime);
                    query.put("state", rs.getString("STATE"));
                    
                    stuckQueries.add(query);
                    totalRunning++;
                }
                
                if (totalRunning > 0) {
                    processes.put("stuck_query_count", totalRunning);
                    processes.put("longest_query_seconds", longestQuerySeconds);
                    processes.put("severity", totalRunning > 3 ? "CRITICAL" : "WARNING");
                    processes.put("queries", stuckQueries);
                    metrics.put("stuck_processes", processes);
                    logger.warn("MySQL: {} stuck processes detected, longest running for {} seconds", totalRunning, longestQuerySeconds);
                } else {
                    processes.put("stuck_query_count", 0);
                    processes.put("longest_query_seconds", 0);
                    metrics.put("stuck_processes", processes);
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check stuck processes (might not support PROCESSLIST)", e);
        }
    }
    
    private void checkInnoDBStatus(Connection connection, Map<String, Object> metrics) {
        try (PreparedStatement stmt = connection.prepareStatement(
            "SELECT OBJECT_SCHEMA, OBJECT_NAME, ENGINE_LOCK_ID, ENGINE_TRANSACTION_ID, THREAD_ID, EVENT_ID, OBJECT_INSTANCE_BEGIN, LOCK_TYPE, LOCK_MODE, LOCK_STATUS, SOURCE, OWNER_THREAD_ID, OWNER_EVENT_ID " +
            "FROM INFORMATION_SCHEMA.INNODB_LOCKS")) {
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, Object> innodb = new LinkedHashMap<>();
                int waitingLocks = 0;
                int grantedLocks = 0;
                
                while (rs.next()) {
                    String lockStatus = rs.getString("LOCK_STATUS");
                    if ("WAITING".equals(lockStatus)) {
                        waitingLocks++;
                    } else {
                        grantedLocks++;
                    }
                }
                
                // Check for deadlocks
                try (PreparedStatement deadlockStmt = connection.prepareStatement(
                    "SHOW ENGINE INNODB STATUS")) {
                    try (ResultSet dlRs = deadlockStmt.executeQuery()) {
                        if (dlRs.next()) {
                            String status = dlRs.getString(3);
                            int deadlockCount = (status != null && status.contains("DEADLOCK")) ? 1 : 0;
                            innodb.put("deadlocks_detected", deadlockCount);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Could not check InnoDB deadlocks", e);
                }
                
                innodb.put("active_transactions", grantedLocks);
                innodb.put("waiting_on_locks", waitingLocks);
                if (waitingLocks > 0) {
                    innodb.put("severity", waitingLocks > 2 ? "CRITICAL" : "WARNING");
                }
                
                metrics.put("innodb", innodb);
                
                if (waitingLocks > 0) {
                    logger.warn("MySQL: {} transactions waiting on InnoDB locks", waitingLocks);
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check InnoDB status (might not support INNODB_LOCKS or not using InnoDB)", e);
        }
    }
    
    private Map<String, String> getMySQLStatusVariables(Connection connection) {
        Map<String, String> statusVars = new LinkedHashMap<>();
        try (PreparedStatement stmt = connection.prepareStatement("SHOW STATUS")) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    statusVars.put(rs.getString("Variable_name"), rs.getString("Value"));
                }
            }
        } catch (Exception e) {
            logger.debug("Could not retrieve MySQL status variables", e);
        }
        return statusVars;
    }

    private Map<String, Object> checkRedisHealth(boolean includeDetails) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", "Redis");

        return performHealthCheck("Redis", details, () -> {
            try {
                // Wrap PING in CompletableFuture with timeout
                String pong = CompletableFuture.supplyAsync(() ->
                    redisTemplate.execute((RedisCallback<String>) connection -> connection.ping()),
                    executorService
                ).get(REDIS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                
                if ("PONG".equals(pong)) {
                    return new HealthCheckResult(true, null, null);
                }
                return new HealthCheckResult(false, null, "Ping returned unexpected response");
            } catch (TimeoutException e) {
                return new HealthCheckResult(false, null, "Redis ping timed out after " + REDIS_TIMEOUT_SECONDS + " seconds");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new HealthCheckResult(false, null, "Redis health check was interrupted");
            } catch (Exception e) {
                throw new IllegalStateException("Redis health check failed", e);
            }
        }, includeDetails);
    }

    private Map<String, Object> checkElasticsearchHealth(boolean includeDetails) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", ELASTICSEARCH_TYPE);

        return performHealthCheck(ELASTICSEARCH_TYPE, details, () -> {
            if (!elasticsearchClientReady || elasticsearchRestClient == null) {
                logger.debug("Elasticsearch RestClient not ready");
                return new HealthCheckResult(false, null, "Elasticsearch client not ready");
            }
            
            try {
                // Execute a simple cluster health request
                Request request = new Request("GET", "/_cluster/health");
                var response = elasticsearchRestClient.performRequest(request);
                
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    logger.debug("{} health check successful", ELASTICSEARCH_TYPE);
                    return new HealthCheckResult(true, null, null);
                }
                return new HealthCheckResult(false, null, "HTTP " + statusCode);
            } catch (java.net.ConnectException e) {
                logger.error("{} connection refused on {}:{}", ELASTICSEARCH_TYPE, elasticsearchHost, elasticsearchPort, e);
                return new HealthCheckResult(false, null, "Connection refused");
            } catch (java.io.IOException e) {
                logger.error("{} IO error: {}", ELASTICSEARCH_TYPE, e.getMessage(), e);
                return new HealthCheckResult(false, null, "IO Error: " + e.getMessage());
            } catch (Exception e) {
                logger.error("{} error: {} - {}", ELASTICSEARCH_TYPE, e.getClass().getSimpleName(), e.getMessage(), e);
                return new HealthCheckResult(false, null, e.getMessage());
            }
        }, includeDetails);
    }

    private Map<String, Object> performHealthCheck(String componentName,
                                                    Map<String, Object> details,
                                                    Supplier<HealthCheckResult> checker,
                                                    boolean includeDetails) {
        Map<String, Object> status = new LinkedHashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            HealthCheckResult result = checker.get();
            long responseTime = System.currentTimeMillis() - startTime;
            
            details.put("responseTimeMs", responseTime);

            if (result.isHealthy) {
                buildHealthyStatus(status, details, componentName, responseTime, result);
            } else {
                buildUnhealthyStatus(status, details, componentName, result, includeDetails);
            }
            
            status.put("details", details);
            return status;
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            return buildExceptionStatus(status, details, componentName, e, includeDetails, responseTime);
        }
    }

    private void buildHealthyStatus(Map<String, Object> status, Map<String, Object> details,
                                    String componentName, long responseTime, HealthCheckResult result) {
        logger.debug("{} health check: UP ({}ms)", componentName, responseTime);
        status.put(STATUS_KEY, STATUS_UP);
    }

    private void buildUnhealthyStatus(Map<String, Object> status, Map<String, Object> details,
                                      String componentName, HealthCheckResult result, boolean includeDetails) {
        String safeError = result.error != null ? result.error : "Health check failed";
        logger.warn("{} health check failed: {}", componentName, safeError);
        status.put(STATUS_KEY, STATUS_DOWN);
        details.put("error", safeError);
        details.put("errorType", "CheckFailed");
    }

    private Map<String, Object> buildExceptionStatus(Map<String, Object> status, Map<String, Object> details,
                                                      String componentName, Exception e, boolean includeDetails, long responseTime) {
        logger.error("{} health check failed with exception: {}", componentName, e.getMessage(), e);
        
        String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
        
        status.put(STATUS_KEY, STATUS_DOWN);
        details.put("responseTimeMs", responseTime);
        details.put("error", errorMessage != null ? errorMessage : "Health check failed");
        status.put("details", details);
        
        return status;
    }

    private boolean isHealthy(Map<String, Object> componentStatus) {
        return STATUS_UP.equals(componentStatus.get(STATUS_KEY));
    }

    private String getMySQLVersion(Connection connection) {
        try (PreparedStatement stmt = connection.prepareStatement(DB_VERSION_QUERY);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (Exception e) {
            logger.debug("Could not retrieve MySQL version", e);
        }
        return null;
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

    private String getRedisVersionWithTimeout() {
        try {
            return CompletableFuture.supplyAsync(this::getRedisVersion, executorService)
                .get(REDIS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.debug("Redis version retrieval timed out");
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Redis version retrieval was interrupted");
            return null;
        } catch (Exception e) {
            logger.debug("Could not retrieve Redis version with timeout", e);
            return null;
        }
    }



    private String extractHost(String jdbcUrl) {
        if (jdbcUrl == null || UNKNOWN_VALUE.equals(jdbcUrl)) {
            return UNKNOWN_VALUE;
        }
        try {
            String withoutPrefix = jdbcUrl.replaceFirst("jdbc:mysql://", "");
            int slashIndex = withoutPrefix.indexOf('/');
            String hostPort = slashIndex > 0
                ? withoutPrefix.substring(0, slashIndex)
                : withoutPrefix;
            int colonIndex = hostPort.indexOf(':');
            return colonIndex > 0 ? hostPort.substring(0, colonIndex) : hostPort;
        } catch (Exception e) {
            logger.debug("Could not extract host from URL", e);
        }
        return UNKNOWN_VALUE;
    }

    private String extractPort(String jdbcUrl) {
        if (jdbcUrl == null || UNKNOWN_VALUE.equals(jdbcUrl)) {
            return UNKNOWN_VALUE;
        }
        try {
            String withoutPrefix = jdbcUrl.replaceFirst("jdbc:mysql://", "");
            int slashIndex = withoutPrefix.indexOf('/');
            String hostPort = slashIndex > 0
                ? withoutPrefix.substring(0, slashIndex)
                : withoutPrefix;
            int colonIndex = hostPort.indexOf(':');
            return colonIndex > 0 ? hostPort.substring(colonIndex + 1) : "3306";
        } catch (Exception e) {
            logger.debug("Could not extract port from URL", e);
        }
        return "3306";
    }

    private String extractDatabaseName(String jdbcUrl) {
        if (jdbcUrl == null || UNKNOWN_VALUE.equals(jdbcUrl)) {
            return UNKNOWN_VALUE;
        }
        try {
            int lastSlash = jdbcUrl.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < jdbcUrl.length() - 1) {
                String afterSlash = jdbcUrl.substring(lastSlash + 1);
                int queryStart = afterSlash.indexOf('?');
                if (queryStart > 0) {
                    return afterSlash.substring(0, queryStart);
                }
                return afterSlash;
            }
        } catch (Exception e) {
            logger.debug("Could not extract database name from URL", e);
        }
        return UNKNOWN_VALUE;
    }

    private static class HealthCheckResult {
        final boolean isHealthy;
        final String version;
        final String error;

        HealthCheckResult(boolean isHealthy, String version, String error) {
            this.isHealthy = isHealthy;
            this.version = version;
            this.error = error;
        }
    }
}
