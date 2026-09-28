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
package com.iemr.common.identity.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityScheme;

/**
 * Tests for the OpenAPI document served at {@code /swagger-ui.html}.
 *
 * <p>
 * The document lists the deployment's own environment URLs and declares bearer
 * authentication, so a "Try it out" call from the docs page reaches the right
 * host with a token attached. Both come from configuration, with a localhost
 * fallback for a developer running the service directly.
 */
class SwaggerConfigTest {

	private final SwaggerConfig config = new SwaggerConfig();

	@Test
	@DisplayName("the configured environment URLs are published as servers")
	void configuredEnvironmentUrlsArePublished() {
		MockEnvironment environment = new MockEnvironment();
		environment.setProperty("api.dev.url", "https://dev.piramalswasthya.org");
		environment.setProperty("api.uat.url", "https://uat.piramalswasthya.org");
		environment.setProperty("api.demo.url", "https://demo.piramalswasthya.org");

		OpenAPI openApi = config.customOpenAPI(environment);

		assertEquals(3, openApi.getServers().size());
		assertEquals("https://dev.piramalswasthya.org", openApi.getServers().get(0).getUrl());
		assertEquals("Dev", openApi.getServers().get(0).getDescription());
		assertEquals("https://uat.piramalswasthya.org", openApi.getServers().get(1).getUrl());
		assertEquals("https://demo.piramalswasthya.org", openApi.getServers().get(2).getUrl());
	}

	@Test
	@DisplayName("an unconfigured environment falls back to localhost rather than publishing nothing")
	void unconfiguredEnvironmentFallsBackToLocalhost() {
		OpenAPI openApi = config.customOpenAPI(new MockEnvironment());

		assertEquals(3, openApi.getServers().size());
		openApi.getServers().forEach(server -> assertEquals("http://localhost:9090", server.getUrl()));
	}

	@Test
	@DisplayName("bearer authentication is declared so the docs page can call a secured endpoint")
	void bearerAuthenticationIsDeclared() {
		OpenAPI openApi = config.customOpenAPI(new MockEnvironment());

		assertEquals(1, openApi.getSecurity().size());
		SecurityScheme scheme = openApi.getComponents().getSecuritySchemes().get("my security");
		assertNotNull(scheme);
		assertEquals(SecurityScheme.Type.HTTP, scheme.getType());
		assertEquals("bearer", scheme.getScheme());
	}

	@Test
	@DisplayName("the document identifies the service")
	void documentIdentifiesTheService() {
		OpenAPI openApi = config.customOpenAPI(new MockEnvironment());

		assertEquals("Identity API", openApi.getInfo().getTitle());
		assertTrue(openApi.getInfo().getDescription().contains("beneficiaries"));
	}
}
