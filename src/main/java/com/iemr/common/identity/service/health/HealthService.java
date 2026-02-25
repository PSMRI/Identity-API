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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see https://www.gnu.org/licenses/.
 */

package com.iemr.common.identity.service.health;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.sql.DataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import jakarta.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
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

    // Status values
    private static final String STATUS_KEY      = "status";
    private static final String STATUS_UP       = "UP";
    private static final String STATUS_DOWN     = "DOWN";
    private static final String STATUS_DEGRADED = "DEGRADED";

    // Severity values
    private static final String SEVERITY_KEY      = "severity";
    private static final String SEVERITY_OK       = "OK";
    private static final String SEVERITY_WARNING  = "WARNING";
    private static final String SEVERITY_CRITICAL = "CRITICAL";

    // Response field keys
    private static final String ERROR_KEY         = "error";
    private static final String MESSAGE_KEY       = "message";
    private static final String RESPONSE_TIME_KEY = "responseTimeMs";

    // Component names
    private static final String MYSQL_COMPONENT         = "MySQL";
    private static final String REDIS_COMPONENT         = "Redis";
    private static final String ELASTICSEARCH_TYPE      = "Elasticsearch";

    // Thresholds
    private static final long RESPONSE_TIME_THRESHOLD_MS         = 2_000L;
    private static final long ADVANCED_CHECKS_THROTTLE_SECONDS   = 30L;
    private static final long ADVANCED_CHECKS_TIMEOUT_MS         = 500L;

    // Diagnostic event codes
    private static final String DIAGNOSTIC_LOCK_WAIT     = "MYSQL_LOCK_WAIT";
    private static final String DIAGNOSTIC_SLOW_QUERIES  = "MYSQL_SLOW_QUERIES";
    private static final String DIAGNOSTIC_POOL_EXHAUSTED = "MYSQL_POOL_EXHAUSTED";
    private static final String DIAGNOSTIC_LOG_TEMPLATE  = "Diagnostic: {}";

    // Elasticsearch constants
    private static final long ELASTICSEARCH_FUNCTIONAL_CHECKS_THROTTLE_MS = 60_000L;
    private static final int  ELASTICSEARCH_CONNECT_TIMEOUT_MS             = 2_000;
    private static final int  ELASTICSEARCH_SOCKET_TIMEOUT_MS              = 2_000;
    private static final int  ELASTICSEARCH_CANARY_TIMEOUT_MS              = 500;
    private static final String ES_CLUSTER_STATUS_YELLOW = "yellow";
    private static final String ES_CLUSTER_STATUS_RED    = "red";

    private static final boolean ADVANCED_HEALTH_CHECKS_ENABLED = true;

    private final DataSource                       dataSource;
    private final ExecutorService                  advancedCheckExecutor;
    private final RedisTemplate<String, Object>    redisTemplate;
    private final String                           elasticsearchHost;
    private final int                              elasticsearchPort;
    private final boolean                          elasticsearchEnabled;
    private final boolean                          elasticsearchIndexingRequired;
    private final String                           elasticsearchTargetIndex;
    private static final ObjectMapper              objectMapper = new ObjectMapper();

    private RestClient elasticsearchRestClient;
    private boolean    elasticsearchClientReady = false;

    private volatile long               lastAdvancedCheckTime     = 0L;
    private volatile AdvancedCheckResult cachedAdvancedCheckResult = null;
    private final ReentrantReadWriteLock advancedCheckLock         = new ReentrantReadWriteLock();
    private final AtomicBoolean          advancedCheckInProgress   = new AtomicBoolean(false);

    private final AtomicReference<ElasticsearchCacheEntry> elasticsearchCache             = new AtomicReference<>(null);
    private volatile long                                   lastElasticsearchFunctionalCheckTime = 0L;
    private final AtomicBoolean                             elasticsearchCheckInProgress          = new AtomicBoolean(false);
    private final AtomicBoolean                             elasticsearchFunctionalCheckInProgress = new AtomicBoolean(false);


    public HealthService(
            DataSource dataSource,
            @Autowired(required = false) RedisTemplate<String, Object> redisTemplate,
            @Value("${elasticsearch.host:localhost}")            String  elasticsearchHost,
            @Value("${elasticsearch.port:9200}")                 int     elasticsearchPort,
            @Value("${elasticsearch.enabled:false}")             boolean elasticsearchEnabled,
            @Value("${elasticsearch.target-index:amrit_data}")   String  elasticsearchTargetIndex,
            @Value("${elasticsearch.indexing-required:false}")   boolean elasticsearchIndexingRequired) {

        this.dataSource = dataSource;
        this.advancedCheckExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "health-advanced-check");
            t.setDaemon(true);
            return t;
        });
        this.redisTemplate              = redisTemplate;
        this.elasticsearchHost          = elasticsearchHost;
        this.elasticsearchPort          = elasticsearchPort;
        this.elasticsearchEnabled       = elasticsearchEnabled;
        this.elasticsearchIndexingRequired = elasticsearchIndexingRequired;
        this.elasticsearchTargetIndex   = (elasticsearchTargetIndex != null) ? elasticsearchTargetIndex : "amrit_data";
    }

    @PostConstruct
    public void init() {
        if (elasticsearchEnabled) {
            initializeElasticsearchClient();
        }
    }

    @jakarta.annotation.PreDestroy
    public void cleanup() {
        advancedCheckExecutor.shutdownNow();
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
                    new HttpHost(elasticsearchHost, elasticsearchPort, "http"))
                .setRequestConfigCallback(cb -> cb
                    .setConnectTimeout(ELASTICSEARCH_CONNECT_TIMEOUT_MS)
                    .setSocketTimeout(ELASTICSEARCH_SOCKET_TIMEOUT_MS))
                .build();
            this.elasticsearchClientReady = true;
            logger.info("Elasticsearch client initialized (connect/socket timeout: {}ms)",
                    ELASTICSEARCH_CONNECT_TIMEOUT_MS);
        } catch (Exception e) {
            logger.warn("Failed to initialize Elasticsearch client: {}", e.getMessage());
            this.elasticsearchClientReady = false;
        }
    }


    public Map<String, Object> checkHealth() {
        Map<String, Object> healthStatus = new LinkedHashMap<>();
        Map<String, Object> components   = new LinkedHashMap<>();
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
            Map<String, Object> esStatus = checkElasticsearchHealth();
            components.put("elasticsearch", esStatus);
            if (!isHealthy(esStatus)) {
                overallHealth = false;
            }
        }

        healthStatus.put(STATUS_KEY, overallHealth ? STATUS_UP : STATUS_DOWN);
        healthStatus.put("timestamp", Instant.now().toString());
        healthStatus.put("components", components);
        logger.info("Health check completed – overall: {}", overallHealth ? STATUS_UP : STATUS_DOWN);
        return healthStatus;
    }

    private Map<String, Object> checkMySQLHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", MYSQL_COMPONENT);

        return performHealthCheck(MYSQL_COMPONENT, details, () -> {
            try (Connection connection = dataSource.getConnection()) {
                if (!connection.isValid(2)) {
                    return new HealthCheckResult(false, "Connection validation failed", false);
                }
                try (PreparedStatement stmt = connection.prepareStatement("SELECT 1 as health_check")) {
                    stmt.setQueryTimeout(3);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next() && rs.getInt(1) == 1) {
                            boolean isDegraded = performAdvancedMySQLChecksWithThrottle();
                            return new HealthCheckResult(true, null, isDegraded);
                        }
                    }
                }
                return new HealthCheckResult(false, "Unexpected query result", false);
            } catch (Exception e) {
                throw new IllegalStateException(MYSQL_COMPONENT + " connection failed: " + e.getMessage(), e);
            }
        });
    }

    private Map<String, Object> checkRedisHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", REDIS_COMPONENT);

        return performHealthCheck(REDIS_COMPONENT, details, () -> {
            try {
                String pong = redisTemplate.execute((RedisCallback<String>) conn -> conn.ping());
                if ("PONG".equals(pong)) {
                    return new HealthCheckResult(true, null, false);
                }
                return new HealthCheckResult(false, "Unexpected ping response", false);
            } catch (Exception e) {
                throw new IllegalStateException("Redis health check failed", e);
            }
        });
    }

    private Map<String, Object> checkElasticsearchHealth() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("type", ELASTICSEARCH_TYPE);
        return performHealthCheck(ELASTICSEARCH_TYPE, details, this::getElasticsearchHealthResult);
    }

    private HealthCheckResult getElasticsearchHealthResult() {
        if (!elasticsearchClientReady || elasticsearchRestClient == null) {
            return new HealthCheckResult(false, "Service unavailable", false);
        }

        long now = System.currentTimeMillis();
        HealthCheckResult cached = getCachedElasticsearchHealth(now);
        if (cached != null) {
            return cached;
        }

        return performElasticsearchHealthCheckWithCache(now);
    }

    private HealthCheckResult getCachedElasticsearchHealth(long now) {
        ElasticsearchCacheEntry cached = elasticsearchCache.get();
        if (cached != null && !cached.isExpired(now)) {
            logger.debug("Returning cached ES health (age: {}ms, status: {})",
                    now - cached.timestamp, cached.result.isHealthy ? STATUS_UP : STATUS_DOWN);
            return cached.result;
        }
        return null;
    }

    private HealthCheckResult performElasticsearchHealthCheckWithCache(long now) {
        // Single-flight: only one thread probes ES; others use stale cache
        if (!elasticsearchCheckInProgress.compareAndSet(false, true)) {
            ElasticsearchCacheEntry fallback = elasticsearchCache.get();
            if (fallback != null) {
                logger.debug("ES check already in progress – using stale cache");
                return fallback.result;
            }
            // On cold start with concurrent requests, return DEGRADED (not DOWN) until first result
            logger.debug("ES check already in progress with no cache – returning DEGRADED");
            return new HealthCheckResult(true, null, true);
        }

        try {
            HealthCheckResult result = performElasticsearchHealthCheck();
            elasticsearchCache.set(new ElasticsearchCacheEntry(result, now));
            return result;
        } catch (Exception e) {
            logger.debug("Elasticsearch health check exception: {}", e.getClass().getSimpleName());
            HealthCheckResult errorResult = new HealthCheckResult(false, "Service unavailable", false);
            elasticsearchCache.set(new ElasticsearchCacheEntry(errorResult, now));
            return errorResult;
        } finally {
            elasticsearchCheckInProgress.set(false);
        }
    }

    private HealthCheckResult performElasticsearchHealthCheck() {
        ClusterHealthStatus healthStatus = getClusterHealthStatus();
        if (healthStatus == null) {
            // Cluster health unavailable; check if index is reachable to determine degradation vs DOWN
            if (indexExists()) {
                logger.debug("Cluster health unavailable but index is reachable – returning DEGRADED");
                return new HealthCheckResult(true, null, true); // DEGRADED: index reachable but cluster health offline
            }
            logger.warn("Cluster health unavailable and index unreachable");
            return new HealthCheckResult(false, "Cluster health unavailable", false);
        }
        if (ES_CLUSTER_STATUS_RED.equals(healthStatus.status)) {
            return new HealthCheckResult(false, "Cluster red", false);
        }

        boolean isDegraded = ES_CLUSTER_STATUS_YELLOW.equals(healthStatus.status);

        String functionalCheckError = shouldRunFunctionalChecks()
                ? performThrottledFunctionalChecksWithError()
                : null;

        if (functionalCheckError != null) {
            return new HealthCheckResult(false, functionalCheckError, false);
        }
        return new HealthCheckResult(true, null, isDegraded);
    }

    private ClusterHealthStatus getClusterHealthStatus() {
        try {
            Request request = new Request("GET", "/_cluster/health");
            applyTimeouts(request, ELASTICSEARCH_CONNECT_TIMEOUT_MS);
            var response = elasticsearchRestClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.debug("Cluster health returned HTTP {}", response.getStatusLine().getStatusCode());
                return null;
            }
            String body = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = objectMapper.readTree(body);
            String status = root.path(STATUS_KEY).asText();
            if (status == null || status.isEmpty()) {
                logger.debug("Could not parse cluster status");
                return null;
            }
            return new ClusterHealthStatus(status);
        } catch (java.net.ConnectException | java.net.SocketTimeoutException e) {
            logger.debug("Elasticsearch network error: {}", e.getClass().getSimpleName());
        } catch (IOException e) {
            logger.debug("Elasticsearch IO error: {}", e.getClass().getSimpleName());
        } catch (Exception e) {
            logger.debug("Elasticsearch health check error: {}", e.getClass().getSimpleName());
        }
        return null;
    }

    private boolean shouldRunFunctionalChecks() {
        return (System.currentTimeMillis() - lastElasticsearchFunctionalCheckTime)
                >= ELASTICSEARCH_FUNCTIONAL_CHECKS_THROTTLE_MS;
    }

    private String performThrottledFunctionalChecksWithError() {
        if (!elasticsearchFunctionalCheckInProgress.compareAndSet(false, true)) {
            logger.debug("Functional checks already in progress – skipping");
            return null;
        }
        try {
            long now = System.currentTimeMillis();

            if (!indexExists()) {
                logger.warn("Functional check failed: index missing");
                lastElasticsearchFunctionalCheckTime = now;
                return "Index missing";
            }

            ReadOnlyCheckResult readOnlyResult = isClusterReadOnly();
            if (readOnlyResult.isReadOnly) {
                logger.warn("Functional check failed: cluster is read-only");
                lastElasticsearchFunctionalCheckTime = now;
                return "Read-only block";
            }
            if (readOnlyResult.isUnableToDetermine) {
                logger.warn("Functional check degraded: unable to determine read-only state");
            }

            CanaryWriteResult canaryResult = performCanaryWriteProbe();
            if (!canaryResult.success) {
                if (elasticsearchIndexingRequired) {
                    logger.warn("Functional check failed: canary write unsuccessful – {}", canaryResult.errorCategory);
                    lastElasticsearchFunctionalCheckTime = now;
                    return "Canary write failed: " + canaryResult.errorCategory;
                } else {
                    logger.debug("Canary write unsuccessful but indexing not required: {}", canaryResult.errorCategory);
                }
            }

            lastElasticsearchFunctionalCheckTime = now;
            return null;
        } finally {
            elasticsearchFunctionalCheckInProgress.set(false);
        }
    }

    private boolean indexExists() {
        try {
            Request request = new Request("HEAD", "/" + elasticsearchTargetIndex);
            applyTimeouts(request, ELASTICSEARCH_CANARY_TIMEOUT_MS);
            var response = elasticsearchRestClient.performRequest(request);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            logger.debug("Index existence check failed: {}", e.getClass().getSimpleName());
            return false;
        }
    }

    private ReadOnlyCheckResult isClusterReadOnly() {
        try {
            Request request = new Request("GET", "/_cluster/settings?include_defaults=true");
            applyTimeouts(request, ELASTICSEARCH_CONNECT_TIMEOUT_MS);
            var response = elasticsearchRestClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.debug("Cluster settings returned HTTP {}", response.getStatusLine().getStatusCode());
                return new ReadOnlyCheckResult(false, true);
            }
            String body = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = objectMapper.readTree(body);
            boolean hasReadOnlyBlock         = hasReadOnlyFlag(root, "read_only");
            boolean hasReadOnlyDeleteBlock   = hasReadOnlyFlag(root, "read_only_allow_delete");
            return new ReadOnlyCheckResult(hasReadOnlyBlock || hasReadOnlyDeleteBlock, false);
        } catch (java.net.SocketTimeoutException e) {
            logger.debug("Read-only check timeout");
        } catch (IOException e) {
            logger.debug("Read-only check IO error: {}", e.getClass().getSimpleName());
        } catch (Exception e) {
            logger.debug("Read-only check failed: {}", e.getClass().getSimpleName());
        }
        return new ReadOnlyCheckResult(false, true);
    }

    private boolean hasReadOnlyFlag(JsonNode root, String flag) {
        String[] paths = {
            "/persistent/cluster/blocks/" + flag,
            "/transient/cluster/blocks/"  + flag,
            "/defaults/cluster/blocks/"   + flag
        };
        for (String path : paths) {
            try {
                JsonNode node = root.at(path);
                if (node != null && !node.isMissingNode() &&
                    ((node.isBoolean() && node.asBoolean()) ||
                     (node.isTextual() && "true".equalsIgnoreCase(node.asText())))) {
                    logger.debug("Found read-only flag at {}", path);
                    return true;
                }
            } catch (Exception e) {
                logger.debug("Error checking JSON pointer {}: {}", path, e.getClass().getSimpleName());
            }
        }
        return false;
    }

    private void applyTimeouts(Request request, int timeoutMs) {
        RequestOptions options = RequestOptions.DEFAULT.toBuilder()
            .setRequestConfig(RequestConfig.custom()
                .setConnectTimeout(timeoutMs)
                .setSocketTimeout(timeoutMs)
                .build())
            .build();
        request.setOptions(options);
    }

    private void performCanaryDelete(String canaryDocId) {
        try {
            Request deleteRequest = new Request("DELETE", "/" + elasticsearchTargetIndex + "/_doc/" + canaryDocId);
            applyTimeouts(deleteRequest, ELASTICSEARCH_CANARY_TIMEOUT_MS);
            elasticsearchRestClient.performRequest(deleteRequest);
        } catch (Exception e) {
            logger.debug("Canary delete warning: {}", e.getClass().getSimpleName());
        }
    }

    private CanaryWriteResult performCanaryWriteProbe() {
        String canaryDocId = "health-check-canary";
        try {
            String canaryBody = "{\"probe\":true,\"timestamp\":\"" + Instant.now() + "\"}";

            // FIX: Use PUT (not POST) for a document with a specific ID
            Request writeRequest = new Request("PUT", "/" + elasticsearchTargetIndex + "/_doc/" + canaryDocId);
            applyTimeouts(writeRequest, ELASTICSEARCH_CANARY_TIMEOUT_MS);
            writeRequest.setEntity(new org.apache.http.entity.StringEntity(canaryBody, StandardCharsets.UTF_8));
            writeRequest.addParameter("refresh", "true");

            var writeResponse = elasticsearchRestClient.performRequest(writeRequest);
            if (writeResponse.getStatusLine().getStatusCode() > 299) {
                logger.debug("Canary write failed with HTTP {}", writeResponse.getStatusLine().getStatusCode());
                return new CanaryWriteResult(false, "Write rejected");
            }

            performCanaryDelete(canaryDocId);
            return new CanaryWriteResult(true, null);
        } catch (java.net.SocketTimeoutException e) {
            logger.debug("Canary probe timeout");
            return new CanaryWriteResult(false, "Timeout");
        } catch (java.net.ConnectException e) {
            logger.debug("Canary probe connection refused");
            return new CanaryWriteResult(false, "Connection refused");
        } catch (Exception e) {
            logger.debug("Canary probe failed: {}", e.getClass().getSimpleName());
            return new CanaryWriteResult(false, "Write failed");
        }
    }


    private Map<String, Object> performHealthCheck(String componentName,
                                                    Map<String, Object> details,
                                                    Supplier<HealthCheckResult> checker) {
        Map<String, Object> status = new LinkedHashMap<>();
        long startTime = System.currentTimeMillis();
        try {
            HealthCheckResult result      = checker.get();
            long              responseTime = System.currentTimeMillis() - startTime;
            details.put(RESPONSE_TIME_KEY, responseTime);

            if (result.isHealthy) {
                buildHealthyStatus(status, componentName, responseTime, result);
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

    private void buildHealthyStatus(Map<String, Object> status,
                                     String componentName, long responseTime, HealthCheckResult result) {
        logger.debug("{} health check: {} ({}ms)",
                componentName, result.isDegraded ? STATUS_DEGRADED : STATUS_UP, responseTime);
        status.put(STATUS_KEY, result.isDegraded ? STATUS_DEGRADED : STATUS_UP);
        status.put(SEVERITY_KEY, determineSeverity(true, responseTime, result.isDegraded));
        if (result.error != null) {
            status.put(MESSAGE_KEY, result.error);
        }
    }

    private void buildUnhealthyStatus(Map<String, Object> status, Map<String, Object> details,
                                       String componentName, HealthCheckResult result) {
        String internalError = (result.error != null) ? result.error : "Health check failed";
        logger.warn("{} health check failed: {}", componentName, internalError);
        status.put(STATUS_KEY,   STATUS_DOWN);
        status.put(SEVERITY_KEY, SEVERITY_CRITICAL);
        // Sanitized outward message – no topology leakage
        details.put(ERROR_KEY,        "Dependency unavailable");
        // For Elasticsearch, sanitize detailed failure reasons; keep real reason in logs only
        String exposedCategory = ELASTICSEARCH_TYPE.equals(componentName) 
            ? "DEPENDENCY_FAILURE"
            : internalError;
        details.put("errorCategory",  exposedCategory);
        details.put("errorType",      "CheckFailed");
    }

    private Map<String, Object> buildExceptionStatus(Map<String, Object> status, Map<String, Object> details,
                                                       String componentName, Exception e, long responseTime) {
        logger.error("{} health check threw exception: {}", componentName, e.getMessage(), e);
        status.put(STATUS_KEY,   STATUS_DOWN);
        status.put(SEVERITY_KEY, SEVERITY_CRITICAL);
        details.put(RESPONSE_TIME_KEY, responseTime);
        // FIX: Sanitize error message – do not expose raw exception detail to consumers
        details.put(ERROR_KEY,       "Dependency unavailable");
        details.put("errorCategory", "CheckException");
        details.put("errorType",     "Exception");
        status.put("details", details);
        return status;
    }

    private String determineSeverity(boolean isHealthy, long responseTimeMs, boolean isDegraded) {
        if (!isHealthy)            return SEVERITY_CRITICAL;
        if (isDegraded)            return SEVERITY_WARNING;
        if (responseTimeMs > RESPONSE_TIME_THRESHOLD_MS) return SEVERITY_WARNING;
        return SEVERITY_OK;
    }

    private boolean isHealthy(Map<String, Object> componentStatus) {
        Object s = componentStatus.get(STATUS_KEY);
        return STATUS_UP.equals(s) || STATUS_DEGRADED.equals(s);
    }

    private boolean performAdvancedMySQLChecksWithThrottle() {
        if (!ADVANCED_HEALTH_CHECKS_ENABLED) {
            return false;
        }

        long currentTime = System.currentTimeMillis();

        // --- Phase 1: try to serve from cache (read lock) ---
        advancedCheckLock.readLock().lock();
        try {
            if (cachedAdvancedCheckResult != null &&
                    (currentTime - lastAdvancedCheckTime) < ADVANCED_CHECKS_THROTTLE_SECONDS * 1_000L) {
                return cachedAdvancedCheckResult.isDegraded;
            }
        } finally {
            advancedCheckLock.readLock().unlock();
        }

        // --- Phase 2: single-flight guard ---
        if (!advancedCheckInProgress.compareAndSet(false, true)) {
            // Another thread is refreshing – return stale cache (safe fallback)
            advancedCheckLock.readLock().lock();
            try {
                return cachedAdvancedCheckResult != null && cachedAdvancedCheckResult.isDegraded;
            } finally {
                advancedCheckLock.readLock().unlock();
            }
        }

        // --- Phase 3: run DB checks outside any lock ---
        try {
            AdvancedCheckResult result = performAdvancedMySQLChecks();

            // --- Phase 4: write-lock for atomic cache update ---
            advancedCheckLock.writeLock().lock();
            try {
                lastAdvancedCheckTime     = System.currentTimeMillis();
                cachedAdvancedCheckResult = result;
                return result.isDegraded;
            } finally {
                advancedCheckLock.writeLock().unlock();
            }
        } finally {
            advancedCheckInProgress.set(false);
        }
    }

    private AdvancedCheckResult performAdvancedMySQLChecks() {
        try (Connection connection = dataSource.getConnection()) {
            return executeAdvancedCheckAsync(connection);
        } catch (Exception e) {
            logger.debug("Failed to get connection for advanced checks: {}", e.getMessage());
            return new AdvancedCheckResult(true);
        }
    }

    private AdvancedCheckResult executeAdvancedCheckAsync(Connection connection) {
        Future<AdvancedCheckResult> future = advancedCheckExecutor.submit(
                () -> performAdvancedCheckLogic(connection));
        try {
            return future.get(ADVANCED_CHECKS_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            logger.debug("Advanced checks timed out – marking degraded");
            future.cancel(true);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.debug("Advanced checks execution failed – marking degraded");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Advanced checks interrupted – marking degraded");
        } catch (Exception e) {
            logger.debug("Advanced checks encountered exception – marking degraded");
        }
        return new AdvancedCheckResult(true);
    }

    private AdvancedCheckResult performAdvancedCheckLogic(Connection connection) {
        try {
            boolean hasIssues = false;

            if (hasLockWaits(connection)) {
                logger.warn(DIAGNOSTIC_LOG_TEMPLATE, DIAGNOSTIC_LOCK_WAIT);
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
            logger.debug("Advanced check logic encountered exception");
            return new AdvancedCheckResult(true);
        }
    }

    private boolean hasLockWaits(Connection connection) {
        String sql =
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST " +
            "WHERE (state = 'Waiting for table metadata lock' " +
            "   OR state = 'Waiting for row lock' " +
            "   OR state = 'Waiting for lock') " +
            "AND user = USER()";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setQueryTimeout(2);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check for lock waits");
        }
        return false;
    }

    private boolean hasSlowQueries(Connection connection) {
        String sql =
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCESSLIST " +
            "WHERE command != 'Sleep' AND time > ? AND user = SUBSTRING_INDEX(USER(), '@', 1)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setQueryTimeout(2);
            stmt.setInt(1, 10); // queries running > 10 seconds
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 3; // more than 3 slow queries
                }
            }
        } catch (Exception e) {
            logger.debug("Could not check for slow queries");
        }
        return false;
    }

    private boolean hasConnectionPoolExhaustion() {
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            try {
                HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
                if (poolMXBean != null) {
                    int activeConnections = poolMXBean.getActiveConnections();
                    int maxPoolSize       = hikariDataSource.getMaximumPoolSize();
                    int threshold         = (int) (maxPoolSize * 0.8);
                    return activeConnections > threshold;
                }
            } catch (Exception e) {
                logger.debug("Could not retrieve HikariCP pool metrics directly");
            }
        }
        return checkPoolMetricsViaJMX();
    }

    private boolean checkPoolMetricsViaJMX() {
        try {
            MBeanServer  mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName   objectName  = new ObjectName("com.zaxxer.hikari:type=Pool (*)");
            var          mBeans      = mBeanServer.queryMBeans(objectName, null);
            for (var mBean : mBeans) {
                if (evaluatePoolMetrics(mBeanServer, mBean.getObjectName())) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not access HikariCP pool metrics via JMX");
        }
        logger.debug("Pool exhaustion check disabled: HikariCP metrics unavailable");
        return false;
    }

    private boolean evaluatePoolMetrics(MBeanServer mBeanServer, ObjectName objectName) {
        try {
            Integer activeConnections = (Integer) mBeanServer.getAttribute(objectName, "ActiveConnections");
            Integer maximumPoolSize   = (Integer) mBeanServer.getAttribute(objectName, "MaximumPoolSize");
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
        AdvancedCheckResult(boolean isDegraded) { this.isDegraded = isDegraded; }
    }

    private static class HealthCheckResult {
        final boolean isHealthy;
        final String  error;
        final boolean isDegraded;
        HealthCheckResult(boolean isHealthy, String error, boolean isDegraded) {
            this.isHealthy  = isHealthy;
            this.error      = error;
            this.isDegraded = isDegraded;
        }
    }

    private static class ElasticsearchCacheEntry {
        final HealthCheckResult result;
        final long              timestamp;
        ElasticsearchCacheEntry(HealthCheckResult result, long timestamp) {
            this.result    = result;
            this.timestamp = timestamp;
        }
        /** UP results cache for 30 s; DOWN results for 5 s for faster recovery. */
        boolean isExpired(long now) {
            long ttlMs = result.isHealthy ? 30_000L : 5_000L;
            return (now - timestamp) >= ttlMs;
        }
    }

    private static class ClusterHealthStatus {
        final String status;
        ClusterHealthStatus(String status) { this.status = status; }
    }

    private static class ReadOnlyCheckResult {
        final boolean isReadOnly;
        final boolean isUnableToDetermine;
        ReadOnlyCheckResult(boolean isReadOnly, boolean isUnableToDetermine) {
            this.isReadOnly          = isReadOnly;
            this.isUnableToDetermine = isUnableToDetermine;
        }
    }

    private static class CanaryWriteResult {
        final boolean success;
        final String  errorCategory;
        CanaryWriteResult(boolean success, String errorCategory) {
            this.success       = success;
            this.errorCategory = errorCategory;
        }
    }
}