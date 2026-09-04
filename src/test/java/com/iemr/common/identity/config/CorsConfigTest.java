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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.config.annotation.CorsRegistration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;

/**
 * Tests for the framework-level CORS registration.
 *
 * <p>
 * These responses carry credentials, so the allow-list has to come from
 * configuration and an unset or blank property must register no origins at all -
 * a wildcard there would let any site read a beneficiary's record with the
 * caller's own cookies.
 */
class CorsConfigTest {

	private CorsConfig config;
	private CorsRegistry registry;
	private CorsRegistration registration;

	@BeforeEach
	void setUp() {
		config = new CorsConfig();
		registry = mock(CorsRegistry.class);
		registration = mock(CorsRegistration.class, RETURNS_SELF);
		when(registry.addMapping(any())).thenReturn(registration);
	}

	private String[] registeredOrigins() {
		ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
		verify(registration).allowedOriginPatterns(captor.capture());
		return captor.getValue();
	}

	@Test
	@DisplayName("the configured origins are registered for every path")
	void configuredOriginsAreRegisteredForEveryPath() {
		ReflectionTestUtils.setField(config, "allowedOrigins",
				"https://amrit.piramalswasthya.org,https://uat.piramalswasthya.org");

		config.addCorsMappings(registry);

		verify(registry).addMapping("/**");
		assertArrayEquals(new String[] { "https://amrit.piramalswasthya.org", "https://uat.piramalswasthya.org" },
				registeredOrigins());
	}

	@Test
	@DisplayName("whitespace and empty entries in the configured list are discarded")
	void whitespaceAndEmptyEntriesAreDiscarded() {
		ReflectionTestUtils.setField(config, "allowedOrigins",
				" https://amrit.piramalswasthya.org , ,https://uat.piramalswasthya.org ");

		config.addCorsMappings(registry);

		assertArrayEquals(new String[] { "https://amrit.piramalswasthya.org", "https://uat.piramalswasthya.org" },
				registeredOrigins());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "   " })
	@DisplayName("a blank allow-list registers no origins rather than a wildcard")
	void blankAllowListRegistersNoOrigins(String configured) {
		ReflectionTestUtils.setField(config, "allowedOrigins", configured);

		config.addCorsMappings(registry);

		assertEquals(0, registeredOrigins().length);
	}

	@Test
	@DisplayName("an unset allow-list registers no origins")
	void unsetAllowListRegistersNoOrigins() {
		config.addCorsMappings(registry);

		assertEquals(0, registeredOrigins().length);
	}

	@Test
	@DisplayName("the methods, headers and credential policy the front end needs are registered")
	void methodsHeadersAndCredentialPolicyAreRegistered() {
		ReflectionTestUtils.setField(config, "allowedOrigins", "https://amrit.piramalswasthya.org");

		config.addCorsMappings(registry);

		verify(registration).allowedMethods("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS");
		verify(registration).exposedHeaders("Authorization", "Jwttoken");
		verify(registration).allowCredentials(true);
		verify(registration).maxAge(3600);
		ArgumentCaptor<String[]> headers = ArgumentCaptor.forClass(String[].class);
		verify(registration).allowedHeaders(headers.capture());
		assertEquals("Authorization", headers.getValue()[0]);
	}
}
