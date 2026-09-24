package com.iemr.common.identity.controller.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.iemr.common.identity.service.health.HealthService;
import com.iemr.common.identity.utils.JwtAuthenticationUtil;

class HealthControllerTest {

	private HealthService healthService;
	private JwtAuthenticationUtil jwtAuthenticationUtil;
	private HealthController healthController;

	@BeforeEach
	void setUp() {
		healthService = mock(HealthService.class);
		jwtAuthenticationUtil = mock(JwtAuthenticationUtil.class);
		healthController = new HealthController(healthService, jwtAuthenticationUtil);
	}

	private Map<String, Object> statusMap(String status) {
		Map<String, Object> map = new HashMap<>();
		map.put("status", status);
		return map;
	}

	@Test
	@DisplayName("UP status maps to HTTP 200")
	void upStatusReturnsOk() {
		when(healthService.checkHealth()).thenReturn(statusMap("UP"));

		ResponseEntity<Map<String, Object>> response = healthController.checkHealth();

		assertEquals(HttpStatus.OK, response.getStatusCode());
		assertEquals("UP", response.getBody().get("status"));
	}

	@Test
	@DisplayName("DEGRADED status still maps to HTTP 200")
	void degradedStatusReturnsOk() {
		when(healthService.checkHealth()).thenReturn(statusMap("DEGRADED"));

		ResponseEntity<Map<String, Object>> response = healthController.checkHealth();

		assertEquals(HttpStatus.OK, response.getStatusCode());
		assertEquals("DEGRADED", response.getBody().get("status"));
	}

	@Test
	@DisplayName("DOWN status maps to HTTP 503")
	void downStatusReturnsServiceUnavailable() {
		when(healthService.checkHealth()).thenReturn(statusMap("DOWN"));

		ResponseEntity<Map<String, Object>> response = healthController.checkHealth();

		assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
	}

	@Test
	@DisplayName("null status is treated as not-DOWN and maps to HTTP 200")
	void nullStatusReturnsOk() {
		when(healthService.checkHealth()).thenReturn(new HashMap<>());

		ResponseEntity<Map<String, Object>> response = healthController.checkHealth();

		assertEquals(HttpStatus.OK, response.getStatusCode());
	}

	@Test
	@DisplayName("service exception yields sanitized 503 body")
	void serviceExceptionReturnsSanitizedError() {
		when(healthService.checkHealth()).thenThrow(new RuntimeException("jdbc://secret@host boom"));

		ResponseEntity<Map<String, Object>> response = healthController.checkHealth();

		assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
		Map<String, Object> body = response.getBody();
		assertNotNull(body);
		assertEquals("DOWN", body.get("status"));
		assertNotNull(body.get("timestamp"));
		assertEquals(2, body.size());
	}
}
