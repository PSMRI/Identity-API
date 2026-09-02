package com.iemr.common.identity.controller.version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class VersionControllerTest {

	private VersionController versionController;

	@BeforeEach
	void setUp() {
		versionController = new VersionController();
	}

	@Test
	@DisplayName("versionInformation returns 200 with all four keys")
	void versionInformationReturnsOkWithAllKeys() {
		ResponseEntity<Map<String, String>> response = versionController.versionInformation();

		assertNotNull(response);
		assertEquals(HttpStatus.OK, response.getStatusCode());
		Map<String, String> body = response.getBody();
		assertNotNull(body);
		assertTrue(body.containsKey("buildTimestamp"));
		assertTrue(body.containsKey("version"));
		assertTrue(body.containsKey("branch"));
		assertTrue(body.containsKey("commitHash"));
	}

	@Test
	@DisplayName("versionInformation never returns null values")
	void versionInformationValuesAreNeverNull() {
		Map<String, String> body = versionController.versionInformation().getBody();

		assertNotNull(body);
		body.values().forEach(org.junit.jupiter.api.Assertions::assertNotNull);
	}

	@Test
	@DisplayName("versionInformation is idempotent across calls")
	void versionInformationIsIdempotent() {
		Map<String, String> first = versionController.versionInformation().getBody();
		Map<String, String> second = versionController.versionInformation().getBody();

		assertEquals(first, second);
	}
}
