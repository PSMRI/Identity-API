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
package com.iemr.common.identity.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.core.Ordered;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for the authentication filter's servlet registration.
 *
 * <p>
 * The filter has to run before anything else and cover every path, or an
 * endpoint could be served without authentication. Both properties are set
 * here, so they are what this test pins.
 */
class FilterConfigTest {

	@Test
	@DisplayName("the authentication filter runs first and covers every path")
	void authenticationFilterRunsFirstAndCoversEveryPath() {
		// Registering it at anything other than highest precedence would let
		// another filter serve a request before authentication runs.
		FilterConfig config = new FilterConfig();
		ReflectionTestUtils.setField(config, "allowedOrigins", "https://amrit.piramalswasthya.org");

		FilterRegistrationBean<JwtUserIdValidationFilter> registration = config
				.jwtUserIdValidationFilter(mock(JwtAuthenticationUtil.class), new CookieUtil());

		assertNotNull(registration.getFilter());
		assertEquals(Ordered.HIGHEST_PRECEDENCE, registration.getOrder());
		Collection<String> patterns = registration.getUrlPatterns();
		assertTrue(patterns.contains("/*"), patterns.toString());
	}

	@Test
	@DisplayName("the configured allow-list is handed to the filter")
	void configuredAllowListIsHandedToTheFilter() {
		FilterConfig config = new FilterConfig();
		ReflectionTestUtils.setField(config, "allowedOrigins", "https://amrit.piramalswasthya.org");

		JwtUserIdValidationFilter filter = config
				.jwtUserIdValidationFilter(mock(JwtAuthenticationUtil.class), new CookieUtil()).getFilter();

		assertEquals("https://amrit.piramalswasthya.org",
				ReflectionTestUtils.getField(filter, "allowedOrigins"));
	}
}
