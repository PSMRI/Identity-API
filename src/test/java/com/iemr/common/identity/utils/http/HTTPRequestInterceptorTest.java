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
package com.iemr.common.identity.utils.http;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.utils.sessionobject.SessionObject;

/**
 * Tests for the request interceptor that keeps a caller's session alive.
 *
 * <p>
 * Every authenticated request refreshes the session's Redis TTL in
 * {@code postHandle}, using the bearer token as the key. The behaviours worth
 * pinning are that the {@code Bearer } prefix is stripped before the token is
 * used as a key - otherwise every request writes a second, never-read entry -
 * and that a Redis failure during the refresh cannot fail a request whose work
 * is already done.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HTTPRequestInterceptorTest {

	@Mock
	private SessionObject sessionObject;

	private HTTPRequestInterceptor interceptor;
	private MockHttpServletRequest request;
	private MockHttpServletResponse response;

	@BeforeEach
	void setUp() {
		interceptor = new HTTPRequestInterceptor();
		interceptor.setSessionObject(sessionObject);
		ReflectionTestUtils.setField(interceptor, "allowedOrigins", "https://*.piramalswasthya.org");
		request = new MockHttpServletRequest();
		response = new MockHttpServletResponse();
	}

	@Test
	@DisplayName("an ordinary request is allowed through")
	void ordinaryRequestIsAllowedThrough() throws Exception {
		request.setMethod("POST");
		request.setRequestURI("/id/getByBenRegId");

		assertTrue(interceptor.preHandle(request, response, new Object()));
	}

	@Test
	@DisplayName("a preflight request is allowed through without inspection")
	void preflightRequestIsAllowedThrough() throws Exception {
		request.setMethod("OPTIONS");
		request.setRequestURI("/id/getByBenRegId");

		assertTrue(interceptor.preHandle(request, response, new Object()));
	}

	@ParameterizedTest
	@ValueSource(strings = { "/version", "/swagger-ui.html", "/api-docs", "/userAuthenticate", "/forgetPassword" })
	@DisplayName("public endpoints are allowed through")
	void publicEndpointsAreAllowedThrough(String uri) throws Exception {
		request.setMethod("GET");
		request.setRequestURI(uri);

		assertTrue(interceptor.preHandle(request, response, new Object()));
	}

	@Test
	@DisplayName("a request routed to the error handler is stopped")
	void requestRoutedToTheErrorHandlerIsStopped() throws Exception {
		request.setMethod("GET");
		request.setRequestURI("/error");

		assertFalse(interceptor.preHandle(request, response, new Object()));
	}

	@Test
	@DisplayName("the bearer prefix is stripped before the token is used as a session key")
	void bearerPrefixIsStrippedBeforeUseAsSessionKey() throws Exception {
		// Keying on "Bearer <token>" would write a parallel entry that the login
		// path never reads, so the session would expire despite activity.
		request.setRequestURI("/id/getByBenRegId");
		request.addHeader("Authorization", "Bearer session-token");
		when(sessionObject.getSessionObject("session-token")).thenReturn("{\"userName\":\"field.worker\"}");

		interceptor.postHandle(request, response, new Object(), null);

		verify(sessionObject).getSessionObject("session-token");
		verify(sessionObject).updateSessionObject("session-token", "{\"userName\":\"field.worker\"}");
	}

	@Test
	@DisplayName("a bare token is used as the session key unchanged")
	void bareTokenIsUsedUnchanged() throws Exception {
		request.setRequestURI("/id/getByBenRegId");
		request.addHeader("Authorization", "session-token");
		when(sessionObject.getSessionObject("session-token")).thenReturn("{}");

		interceptor.postHandle(request, response, new Object(), null);

		verify(sessionObject).updateSessionObject("session-token", "{}");
	}

	@Test
	@DisplayName("a request with no authorization header refreshes nothing")
	void requestWithNoAuthorizationRefreshesNothing() throws Exception {
		request.setRequestURI("/health");

		interceptor.postHandle(request, response, new Object(), null);

		verify(sessionObject, never()).updateSessionObject(anyString(), anyString());
	}

	@Test
	@DisplayName("a failing session refresh does not fail the request")
	void failingSessionRefreshDoesNotFailTheRequest() throws Exception {
		// The response has already been produced by this point.
		request.setRequestURI("/id/getByBenRegId");
		request.addHeader("Authorization", "Bearer session-token");
		when(sessionObject.getSessionObject(anyString()))
				.thenThrow(new IllegalStateException("connection refused"));

		interceptor.postHandle(request, response, new Object(), null);
	}

	@Test
	@DisplayName("completion is a no-op that cannot fail a served request")
	void completionIsANoOp() throws Exception {
		interceptor.afterCompletion(request, response, new Object(), null);
	}
}
