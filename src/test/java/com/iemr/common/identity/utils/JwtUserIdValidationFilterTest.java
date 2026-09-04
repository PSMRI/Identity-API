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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.iemr.common.identity.exception.IEMRException;
import com.iemr.common.identity.utils.http.AuthorizationHeaderRequestWrapper;

/**
 * Tests for the authentication filter in front of every identity endpoint.
 *
 * <p>
 * This filter is the whole authentication boundary, so the tests are written
 * around what must not get through: a request with no token, a request whose
 * token fails validation, and a request that merely claims to be a mobile
 * client. It also has to keep letting the health and version probes through
 * unauthenticated - the load balancer depends on that - and must reflect a CORS
 * origin only when it matches the configured allow-list, since the responses
 * carry credentials.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JwtUserIdValidationFilterTest {

	@Mock
	private JwtAuthenticationUtil jwtAuthenticationUtil;
	@Mock
	private FilterChain filterChain;

	private JwtUserIdValidationFilter filter;
	private MockHttpServletRequest request;
	private MockHttpServletResponse response;

	private static final String ALLOWED_ORIGINS = "https://*.piramalswasthya.org,https://amrit.example.org";

	@BeforeEach
	void setUp() {
		filter = new JwtUserIdValidationFilter(jwtAuthenticationUtil, ALLOWED_ORIGINS, new CookieUtil());
		request = new MockHttpServletRequest();
		response = new MockHttpServletResponse();
		request.setMethod("POST");
		request.setRequestURI("/id/getByBenRegId");
		request.setServletPath("/id/getByBenRegId");
	}

	@Nested
	@DisplayName("authenticating a request")
	class AuthenticatingARequest {

		@Test
		@DisplayName("a request carrying a valid token in a cookie is let through")
		void validTokenInACookieIsLetThrough() throws Exception {
			request.setCookies(new Cookie("Jwttoken", "valid-token"));
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken("valid-token")).thenReturn(true);

			filter.doFilter(request, response, filterChain);

			verify(filterChain).doFilter(any(AuthorizationHeaderRequestWrapper.class), any());
			assertEquals(HttpServletResponse.SC_OK, response.getStatus());
		}

		@Test
		@DisplayName("a request carrying a valid token in a header is let through")
		void validTokenInAHeaderIsLetThrough() throws Exception {
			request.addHeader("JwtToken", "valid-token");
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken("valid-token")).thenReturn(true);

			filter.doFilter(request, response, filterChain);

			verify(filterChain).doFilter(any(AuthorizationHeaderRequestWrapper.class), any());
		}

		@Test
		@DisplayName("a cookie token wins over a header token")
		void cookieTokenWinsOverHeaderToken() throws Exception {
			request.setCookies(new Cookie("Jwttoken", "cookie-token"));
			request.addHeader("JwtToken", "header-token");
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken(anyString())).thenReturn(true);

			filter.doFilter(request, response, filterChain);

			verify(jwtAuthenticationUtil).validateUserIdAndJwtToken("cookie-token");
		}

		@Test
		@DisplayName("a request with no token at all is refused")
		void requestWithNoTokenIsRefused() throws Exception {
			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
			verify(filterChain, never()).doFilter(any(), any());
		}

		@Test
		@DisplayName("a request whose token fails validation is refused")
		void requestWithAnInvalidTokenIsRefused() throws Exception {
			request.addHeader("JwtToken", "invalid-token");
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken("invalid-token")).thenReturn(false);

			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
			verify(filterChain, never()).doFilter(any(), any());
		}

		@Test
		@DisplayName("a validation error is refused rather than treated as authenticated")
		void validationErrorIsRefused() throws Exception {
			request.addHeader("JwtToken", "some-token");
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken(anyString()))
					.thenThrow(new IEMRException("Invalid User ID."));

			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
			verify(filterChain, never()).doFilter(any(), any());
		}

		@Test
		@DisplayName("a userId cookie sent by the client is cleared instead of trusted")
		void userIdCookieSentByTheClientIsCleared() throws Exception {
			// The user id is taken from the signed token; an attacker-supplied
			// cookie of the same name must not survive the request.
			request.setCookies(new Cookie("userId", "31"), new Cookie("Jwttoken", "valid-token"));
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken("valid-token")).thenReturn(true);

			filter.doFilter(request, response, filterChain);

			Cookie cleared = response.getCookie("userId");
			assertEquals(0, cleared.getMaxAge());
			assertNull(cleared.getValue());
			assertTrue(cleared.isHttpOnly());
			assertTrue(cleared.getSecure());
		}
	}

	@Nested
	@DisplayName("mobile clients")
	class MobileClients {

		@ParameterizedTest
		@ValueSource(strings = { "okhttp/4.9.0", "Java/17.0.2", "okhttp" })
		@DisplayName("a mobile client presenting an authorization header is let through for downstream checks")
		void mobileClientWithAnAuthorizationHeaderIsLetThrough(String userAgent) throws Exception {
			request.addHeader("User-Agent", userAgent);
			request.addHeader("Authorization", "Bearer session-token");

			filter.doFilter(request, response, filterChain);

			verify(filterChain).doFilter(request, response);
		}

		@Test
		@DisplayName("a mobile client with no authorization header is refused")
		void mobileClientWithNoAuthorizationHeaderIsRefused() throws Exception {
			request.addHeader("User-Agent", "okhttp/4.9.0");

			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
		}

		@ParameterizedTest
		@ValueSource(strings = { "Mozilla/5.0", "curl/8.0.1", "PostmanRuntime/7.29.0" })
		@DisplayName("a browser or generic client cannot use the mobile bypass")
		void browserClientCannotUseTheMobileBypass(String userAgent) throws Exception {
			// Otherwise any caller could skip JWT validation by sending a bearer
			// header and a plausible user agent.
			request.addHeader("User-Agent", userAgent);
			request.addHeader("Authorization", "Bearer session-token");

			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
			verify(filterChain, never()).doFilter(any(), any());
		}
	}

	@Nested
	@DisplayName("public probes")
	class PublicProbes {

		@ParameterizedTest
		@ValueSource(strings = { "/health", "/version" })
		@DisplayName("the load balancer's probes are served without a token")
		void probesAreServedWithoutAToken(String path) throws Exception {
			request.setServletPath(path);
			request.setRequestURI(path);

			filter.doFilter(request, response, filterChain);

			verify(filterChain).doFilter(request, response);
			verify(jwtAuthenticationUtil, never()).validateUserIdAndJwtToken(anyString());
		}

		@ParameterizedTest
		@ValueSource(strings = { "/identity/health", "/identity/version" })
		@DisplayName("the probes are also recognised behind a context path")
		void probesAreRecognisedBehindAContextPath(String uri) throws Exception {
			request.setRequestURI(uri);
			request.setServletPath("/other");

			filter.doFilter(request, response, filterChain);

			verify(filterChain).doFilter(request, response);
		}

		@Test
		@DisplayName("a preflight request is answered without authentication")
		void preflightRequestIsAnsweredWithoutAuthentication() throws Exception {
			request.setMethod("OPTIONS");

			filter.doFilter(request, response, filterChain);

			assertEquals(HttpServletResponse.SC_OK, response.getStatus());
			verify(filterChain, never()).doFilter(any(), any());
		}
	}

	@Nested
	@DisplayName("CORS reflection")
	class CorsReflection {

		@ParameterizedTest
		@ValueSource(strings = { "https://amrit.piramalswasthya.org", "https://uat.piramalswasthya.org",
				"https://amrit.example.org" })
		@DisplayName("an allowed origin is reflected back with credentials permitted")
		void allowedOriginIsReflected(String origin) throws Exception {
			request.addHeader("Origin", origin);
			request.setCookies(new Cookie("Jwttoken", "valid-token"));
			when(jwtAuthenticationUtil.validateUserIdAndJwtToken(anyString())).thenReturn(true);

			filter.doFilter(request, response, filterChain);

			assertEquals(origin, response.getHeader("Access-Control-Allow-Origin"));
			assertEquals("true", response.getHeader("Access-Control-Allow-Credentials"));
			assertEquals("Origin", response.getHeader("Vary"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "https://evil.example.com", "http://amrit.piramalswasthya.org.evil.com",
				"null" })
		@DisplayName("an origin outside the allow-list is not reflected")
		void originOutsideTheAllowListIsNotReflected(String origin) throws Exception {
			// These responses carry credentials, so reflecting an arbitrary
			// origin would hand a beneficiary's record to any site.
			request.addHeader("Origin", origin);

			filter.doFilter(request, response, filterChain);

			assertNull(response.getHeader("Access-Control-Allow-Origin"));
		}

		@Test
		@DisplayName("a request with no origin gets no CORS headers")
		void requestWithNoOriginGetsNoCorsHeaders() throws Exception {
			filter.doFilter(request, response, filterChain);

			assertNull(response.getHeader("Access-Control-Allow-Origin"));
		}

		@Test
		@DisplayName("with no allow-list configured, no origin is reflected")
		void withNoAllowListConfiguredNoOriginIsReflected() throws Exception {
			JwtUserIdValidationFilter unconfigured = new JwtUserIdValidationFilter(jwtAuthenticationUtil, "",
					new CookieUtil());
			request.addHeader("Origin", "https://amrit.piramalswasthya.org");

			unconfigured.doFilter(request, response, filterChain);

			assertNull(response.getHeader("Access-Control-Allow-Origin"));
		}

		@Test
		@DisplayName("an allowed preflight request is answered with the permitted methods and headers")
		void allowedPreflightIsAnsweredWithPermittedMethodsAndHeaders() throws Exception {
			request.setMethod("OPTIONS");
			request.addHeader("Origin", "https://amrit.piramalswasthya.org");

			filter.doFilter(request, response, filterChain);

			assertTrue(response.getHeader("Access-Control-Allow-Methods").contains("POST"));
			assertTrue(response.getHeader("Access-Control-Allow-Headers").contains("JwtToken"));
		}
	}
}
