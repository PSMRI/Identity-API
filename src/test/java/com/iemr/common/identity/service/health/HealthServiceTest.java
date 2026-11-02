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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for the dependency health check behind {@code /health}.
 *
 * <p>
 * This endpoint is what the load balancer and the on-call dashboard read, so
 * the distinctions it draws matter operationally: a slow-but-working dependency
 * has to report DEGRADED rather than DOWN so instances are not pulled out of
 * rotation, an unavailable dependency has to report DOWN, and neither may leak
 * cluster topology or raw exception text into a response that is served
 * unauthenticated.
 *
 * <p>
 * Every probe is driven through mocked JDBC, Redis and Elasticsearch clients;
 * nothing here contacts a real dependency.
 */
class HealthServiceTest {

	private DataSource dataSource;
	private RedisTemplate<String, Object> redisTemplate;
	private RestClient restClient;

	@BeforeEach
	void setUp() {
		dataSource = mock(DataSource.class);
		redisTemplate = mock(RedisTemplate.class);
		restClient = mock(RestClient.class);
	}

	/** Builds the service with Elasticsearch switched off. */
	private HealthService serviceWithoutElasticsearch(RedisTemplate<String, Object> redis) {
		return new HealthService(dataSource, redis, "localhost", 9200, false, "amrit_data", false);
	}

	/** Builds the service with a mocked Elasticsearch client already in place. */
	private HealthService serviceWithElasticsearch(boolean indexingRequired) {
		HealthService service = new HealthService(dataSource, null, "localhost", 9200, true, "amrit_data",
				indexingRequired);
		ReflectionTestUtils.setField(service, "elasticsearchRestClient", restClient);
		ReflectionTestUtils.setField(service, "elasticsearchClientReady", true);
		return service;
	}

	/**
	 * Stubs a JDBC connection whose {@code SELECT 1} probe succeeds and whose
	 * diagnostic {@code PROCESSLIST} queries report a quiet server.
	 */
	private Connection stubHealthyDatabase() throws SQLException {
		return stubDatabase(0);
	}

	/**
	 * Stubs a working JDBC connection where the diagnostic queries report
	 * {@code diagnosticCount} waiting or slow sessions.
	 */
	private Connection stubDatabase(int diagnosticCount) throws SQLException {
		Connection connection = mock(Connection.class);
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.isValid(anyInt())).thenReturn(true);
		when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
			String sql = invocation.getArgument(0);
			return sql.contains("PROCESSLIST") ? countingStatement(diagnosticCount) : countingStatement(1);
		});
		return connection;
	}

	/** A statement whose single-column result is {@code count}. */
	private PreparedStatement countingStatement(int count) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(statement.executeQuery()).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getInt(1)).thenReturn(count);
		return statement;
	}

	private void stubPong(String reply) {
		when(redisTemplate.execute(any(RedisCallback.class))).thenReturn(reply);
	}

	private Response response(int statusCode, String body) throws IOException {
		Response response = mock(Response.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, "");
		when(response.getStatusLine()).thenReturn(statusLine);
		HttpEntity entity = mock(HttpEntity.class);
		when(entity.getContent()).thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
		when(response.getEntity()).thenReturn(entity);
		return response;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> component(Map<String, Object> health, String name) {
		return (Map<String, Object>) ((Map<String, Object>) health.get("components")).get(name);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> details(Map<String, Object> component) {
		return (Map<String, Object>) component.get("details");
	}

	@Nested
	@DisplayName("overall status")
	class OverallStatus {

		@Test
		@DisplayName("a working database with no other dependency configured reports UP")
		void workingDatabaseReportsUp() throws Exception {
			stubHealthyDatabase();
			HealthService service = serviceWithoutElasticsearch(null);

			Map<String, Object> health = service.checkHealth();

			assertEquals("UP", health.get("status"));
			assertNotNull(health.get("timestamp"));
			assertEquals("UP", component(health, "mysql").get("status"));
		}

		@Test
		@DisplayName("a degraded dependency keeps the instance in rotation")
		void degradedDependencyKeepsInstanceInRotation() throws Exception {
			// DEGRADED must not become an overall DOWN: the load balancer would
			// pull a working instance out of service.
			stubDatabase(5);
			HealthService service = serviceWithoutElasticsearch(null);

			Map<String, Object> health = service.checkHealth();

			assertEquals("UP", health.get("status"));
			assertEquals("DEGRADED", component(health, "mysql").get("status"));
			assertEquals("WARNING", component(health, "mysql").get("severity"));
		}

		@Test
		@DisplayName("an unreachable database reports DOWN overall")
		void unreachableDatabaseReportsDown() throws Exception {
			when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));
			HealthService service = serviceWithoutElasticsearch(null);

			Map<String, Object> health = service.checkHealth();

			assertEquals("DOWN", health.get("status"));
			assertEquals("DOWN", component(health, "mysql").get("status"));
			assertEquals("CRITICAL", component(health, "mysql").get("severity"));
		}

		@Test
		@DisplayName("Redis is only reported when it is configured")
		void redisIsOnlyReportedWhenConfigured() throws Exception {
			stubHealthyDatabase();

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			assertFalse(((Map<?, ?>) health.get("components")).containsKey("redis"));
		}

		@Test
		@DisplayName("Elasticsearch is only reported when it is enabled")
		void elasticsearchIsOnlyReportedWhenEnabled() throws Exception {
			stubHealthyDatabase();

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			assertFalse(((Map<?, ?>) health.get("components")).containsKey("elasticsearch"));
		}

	}

	@Nested
	@DisplayName("database probe")
	class DatabaseProbe {

		@Test
		@DisplayName("a connection that fails validation is reported DOWN without running a query")
		void invalidConnectionIsReportedDown() throws Exception {
			Connection connection = mock(Connection.class);
			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.isValid(anyInt())).thenReturn(false);

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			assertEquals("DOWN", component(health, "mysql").get("status"));
			verify(connection, never()).prepareStatement(anyString());
		}

		@Test
		@DisplayName("a probe that returns nothing is reported DOWN")
		void probeReturningNothingIsReportedDown() throws Exception {
			Connection connection = mock(Connection.class);
			PreparedStatement statement = mock(PreparedStatement.class);
			ResultSet resultSet = mock(ResultSet.class);
			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.isValid(anyInt())).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);
			when(statement.executeQuery()).thenReturn(resultSet);
			when(resultSet.next()).thenReturn(false);

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			assertEquals("DOWN", component(health, "mysql").get("status"));
			assertEquals("Unexpected query result", details(component(health, "mysql")).get("errorCategory"));
		}

		@Test
		@DisplayName("the probe is given a query timeout so a stuck server cannot hang the endpoint")
		void probeIsGivenAQueryTimeout() throws Exception {
			Connection connection = mock(Connection.class);
			PreparedStatement statement = countingStatement(1);
			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.isValid(anyInt())).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenReturn(statement);

			serviceWithoutElasticsearch(null).checkHealth();

			verify(statement, org.mockito.Mockito.atLeastOnce()).setQueryTimeout(3);
		}

		@Test
		@DisplayName("a raw exception message is never exposed in the response")
		void rawExceptionMessageIsNeverExposed() throws Exception {
			when(dataSource.getConnection())
					.thenThrow(new SQLException("Access denied for user 'amrit'@'10.1.2.3' to database 'db_iemr'"));

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			Map<String, Object> details = details(component(health, "mysql"));
			assertEquals("Dependency unavailable", details.get("error"));
			assertFalse(details.toString().contains("10.1.2.3"));
			assertFalse(details.toString().contains("db_iemr"));
		}

		@Test
		@DisplayName("a diagnostic query that fails does not turn a working database into a degraded one")
		void failingDiagnosticQueryDoesNotDegradeAWorkingDatabase() throws Exception {
			Connection connection = mock(Connection.class);
			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.isValid(anyInt())).thenReturn(true);
			when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
				String sql = invocation.getArgument(0);
				if (sql.contains("PROCESSLIST")) {
					throw new SQLException("INFORMATION_SCHEMA not permitted");
				}
				return countingStatement(1);
			});

			Map<String, Object> health = serviceWithoutElasticsearch(null).checkHealth();

			assertEquals("UP", component(health, "mysql").get("status"));
		}
	}

	@Nested
	@DisplayName("Redis probe")
	class RedisProbe {

		@Test
		@DisplayName("a server that answers PONG is reported UP")
		void pongIsReportedUp() throws Exception {
			stubHealthyDatabase();
			stubPong("PONG");

			Map<String, Object> health = serviceWithoutElasticsearch(redisTemplate).checkHealth();

			assertEquals("UP", component(health, "redis").get("status"));
			assertEquals("UP", health.get("status"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "", "WRONGPASS", "pong" })
		@DisplayName("any answer other than PONG is reported DOWN")
		void anyOtherAnswerIsReportedDown(String reply) throws Exception {
			stubHealthyDatabase();
			stubPong(reply);

			Map<String, Object> health = serviceWithoutElasticsearch(redisTemplate).checkHealth();

			assertEquals("DOWN", component(health, "redis").get("status"));
			assertEquals("DOWN", health.get("status"));
		}

		@Test
		@DisplayName("an unreachable Redis is reported DOWN with a sanitised message")
		void unreachableRedisIsReportedDown() throws Exception {
			stubHealthyDatabase();
			when(redisTemplate.execute(any(RedisCallback.class)))
					.thenThrow(new IllegalStateException("Unable to connect to redis-01.internal:6379"));

			Map<String, Object> health = serviceWithoutElasticsearch(redisTemplate).checkHealth();

			Map<String, Object> details = details(component(health, "redis"));
			assertEquals("DOWN", component(health, "redis").get("status"));
			assertEquals("Dependency unavailable", details.get("error"));
			assertFalse(details.toString().contains("redis-01.internal"));
		}
	}

	@Nested
	@DisplayName("Elasticsearch probe")
	class ElasticsearchProbe {

		@BeforeEach
		void stubDatabase() throws SQLException {
			stubHealthyDatabase();
		}

		/**
		 * The probe issues up to four requests; this routes each by method and
		 * path so a test can control them independently.
		 */
		private void stubCluster(String clusterStatus, boolean indexPresent, String settingsBody,
				Integer canaryStatusCode) throws IOException {
			when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
				Request request = invocation.getArgument(0);
				String endpoint = request.getEndpoint();
				if ("/_cluster/health".equals(endpoint)) {
					if (clusterStatus == null) {
						throw new IOException("cluster health unavailable");
					}
					return response(200, "{\"status\":\"" + clusterStatus + "\"}");
				}
				if ("HEAD".equals(request.getMethod())) {
					if (!indexPresent) {
						throw new IOException("index missing");
					}
					return response(200, "");
				}
				if (endpoint.startsWith("/_cluster/settings")) {
					if (settingsBody == null) {
						throw new IOException("settings unavailable");
					}
					return response(200, settingsBody);
				}
				if ("PUT".equals(request.getMethod())) {
					if (canaryStatusCode == null) {
						throw new java.net.SocketTimeoutException("canary timeout");
					}
					return response(canaryStatusCode, "{}");
				}
				return response(200, "{}");
			});
		}

		private static final String NO_READ_ONLY_BLOCKS = "{\"persistent\":{},\"transient\":{},\"defaults\":{}}";

		@Test
		@DisplayName("a green cluster that accepts a canary write is reported UP")
		void greenClusterAcceptingWritesIsReportedUp() throws Exception {
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("UP", component(health, "elasticsearch").get("status"));
			assertEquals("UP", health.get("status"));
		}

		@Test
		@DisplayName("a yellow cluster is reported DEGRADED, not DOWN")
		void yellowClusterIsReportedDegraded() throws Exception {
			// Yellow means unassigned replicas - searches and writes still work.
			stubCluster("yellow", true, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DEGRADED", component(health, "elasticsearch").get("status"));
			assertEquals("WARNING", component(health, "elasticsearch").get("severity"));
			assertEquals("UP", health.get("status"));
		}

		@Test
		@DisplayName("a red cluster is reported DOWN")
		void redClusterIsReportedDown() throws Exception {
			stubCluster("red", true, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
			assertEquals("DOWN", health.get("status"));
		}

		@Test
		@DisplayName("an unreadable cluster health with a reachable index is DEGRADED rather than DOWN")
		void unreadableClusterHealthWithReachableIndexIsDegraded() throws Exception {
			stubCluster(null, true, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DEGRADED", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("an unreadable cluster health with an unreachable index is DOWN")
		void unreadableClusterHealthWithUnreachableIndexIsDown() throws Exception {
			stubCluster(null, false, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a missing target index is reported DOWN")
		void missingTargetIndexIsReportedDown() throws Exception {
			stubCluster("green", false, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@ParameterizedTest
		@CsvSource(delimiter = '|', value = {
				"{\"persistent\":{\"cluster\":{\"blocks\":{\"read_only\":true}}}}",
				"{\"transient\":{\"cluster\":{\"blocks\":{\"read_only\":\"true\"}}}}",
				"{\"defaults\":{\"cluster\":{\"blocks\":{\"read_only_allow_delete\":true}}}}" })
		@DisplayName("a read-only block is reported DOWN wherever the setting is applied")
		void readOnlyBlockIsReportedDown(String settingsBody) throws Exception {
			// A read-only cluster still answers cluster health as green, so only
			// the settings probe catches the disk-watermark case.
			stubCluster("green", true, settingsBody, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("an unreadable settings response does not by itself fail the check")
		void unreadableSettingsDoesNotFailTheCheck() throws Exception {
			stubCluster("green", true, null, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("UP", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a rejected canary write is reported DOWN when indexing is required")
		void rejectedCanaryWriteIsReportedDownWhenIndexingRequired() throws Exception {
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, 503);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a rejected canary write is tolerated when indexing is not required")
		void rejectedCanaryWriteIsToleratedWhenIndexingNotRequired() throws Exception {
			// Search-only deployments index from a separate job, so a write
			// failure is not an outage for this service.
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, 503);

			Map<String, Object> health = serviceWithElasticsearch(false).checkHealth();

			assertEquals("UP", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a canary write that times out is reported DOWN when indexing is required")
		void timedOutCanaryWriteIsReportedDown() throws Exception {
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, null);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("the canary document is deleted again after a successful write")
		void canaryDocumentIsDeletedAgain() throws Exception {
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, 201);

			serviceWithElasticsearch(true).checkHealth();

			ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
			verify(restClient, org.mockito.Mockito.atLeastOnce()).performRequest(captor.capture());
			assertTrue(captor.getAllValues().stream().anyMatch(request -> "DELETE".equals(request.getMethod())),
					"the canary document was left behind in the index");
		}

		@Test
		@DisplayName("the failure reason is not leaked to an unauthenticated caller")
		void failureReasonIsNotLeaked() throws Exception {
			stubCluster("red", true, NO_READ_ONLY_BLOCKS, 201);

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			Map<String, Object> details = details(component(health, "elasticsearch"));
			assertEquals("Dependency unavailable", details.get("error"));
			assertEquals("DEPENDENCY_FAILURE", details.get("errorCategory"));
			assertFalse(details.toString().contains("Cluster red"));
		}

		@Test
		@DisplayName("a repeated check is served from cache rather than re-probing the cluster")
		void repeatedCheckIsServedFromCache() throws Exception {
			stubCluster("green", true, NO_READ_ONLY_BLOCKS, 201);
			HealthService service = serviceWithElasticsearch(true);

			service.checkHealth();
			int requestsAfterFirstCheck = org.mockito.Mockito.mockingDetails(restClient).getInvocations().size();
			service.checkHealth();

			assertEquals(requestsAfterFirstCheck,
					org.mockito.Mockito.mockingDetails(restClient).getInvocations().size());
		}

		@Test
		@DisplayName("an enabled but uninitialised client is reported DOWN rather than skipped")
		void uninitialisedClientIsReportedDown() throws Exception {
			HealthService service = new HealthService(dataSource, null, "localhost", 9200, true, "amrit_data", false);
			ReflectionTestUtils.setField(service, "elasticsearchClientReady", true);

			Map<String, Object> health = service.checkHealth();

			assertEquals("DOWN", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a cluster health response that is not 200 counts as unreadable")
		void nonOkClusterHealthCountsAsUnreadable() throws Exception {
			when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
				Request request = invocation.getArgument(0);
				if ("/_cluster/health".equals(request.getEndpoint())) {
					return response(503, "");
				}
				return response(200, "");
			});

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DEGRADED", component(health, "elasticsearch").get("status"));
		}

		@Test
		@DisplayName("a cluster health response with no status field counts as unreadable")
		void clusterHealthWithNoStatusCountsAsUnreadable() throws Exception {
			when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
				Request request = invocation.getArgument(0);
				if ("/_cluster/health".equals(request.getEndpoint())) {
					return response(200, "{\"cluster_name\":\"amrit\"}");
				}
				return response(200, "");
			});

			Map<String, Object> health = serviceWithElasticsearch(true).checkHealth();

			assertEquals("DEGRADED", component(health, "elasticsearch").get("status"));
		}
	}

	@Nested
	@DisplayName("lifecycle")
	class Lifecycle {

		@Test
		@DisplayName("start-up builds an Elasticsearch client only when the integration is enabled")
		void startUpBuildsClientOnlyWhenEnabled() {
			HealthService disabled = serviceWithoutElasticsearch(null);
			disabled.init();
			assertFalse((Boolean) ReflectionTestUtils.getField(disabled, "elasticsearchClientReady"));

			HealthService enabled = new HealthService(dataSource, null, "localhost", 9200, true, "amrit_data", false);
			enabled.init();
			assertTrue((Boolean) ReflectionTestUtils.getField(enabled, "elasticsearchClientReady"));
			enabled.cleanup();
		}

		@Test
		@DisplayName("shutdown releases the checker thread and the Elasticsearch client")
		void shutdownReleasesResources() throws Exception {
			HealthService service = serviceWithElasticsearch(false);

			service.cleanup();

			verify(restClient).close();
		}

		@Test
		@DisplayName("a client that fails to close does not break shutdown")
		void clientThatFailsToCloseDoesNotBreakShutdown() throws Exception {
			HealthService service = serviceWithElasticsearch(false);
			org.mockito.Mockito.doThrow(new IOException("already closed")).when(restClient).close();

			service.cleanup();

			verify(restClient).close();
		}

		@Test
		@DisplayName("an absent target index name falls back to the default index")
		void absentTargetIndexFallsBackToDefault() {
			HealthService service = new HealthService(dataSource, null, "localhost", 9200, false, null, false);

			assertEquals("amrit_data", ReflectionTestUtils.getField(service, "elasticsearchTargetIndex"));
		}
	}
}
