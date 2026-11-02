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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletRequest;

import com.iemr.common.identity.domain.User;
import com.iemr.common.identity.exception.IEMRException;

import io.jsonwebtoken.Claims;
import jakarta.servlet.http.Cookie;

/**
 * Tests for the token-to-user check the filter delegates to.
 *
 * <p>
 * A signature-valid token is not sufficient: the user it names has to still
 * exist and not be soft-deleted, or a token issued before an account was
 * removed would keep working. The lookup is cached in Redis for 30 minutes, so
 * these tests also pin that the cache is consulted before the database and
 * populated after a miss.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JwtAuthenticationUtilTest {

	@Mock
	private JwtUtil jwtUtil;
	@Mock
	private RedisTemplate<String, Object> redisTemplate;
	@Mock
	private ValueOperations<String, Object> valueOperations;
	@Mock
	private JdbcTemplate jdbcTemplate;
	@Mock
	private Claims claims;

	private JwtAuthenticationUtil authUtil;
	private MockHttpServletRequest request;

	@BeforeEach
	void setUp() {
		authUtil = new JwtAuthenticationUtil(new CookieUtil(), jwtUtil, redisTemplate, jdbcTemplate);
		request = new MockHttpServletRequest();
		when(redisTemplate.opsForValue()).thenReturn(valueOperations);
	}

	private void stubValidTokenFor(String userId, String username) {
		when(jwtUtil.validateToken(anyString())).thenReturn(claims);
		when(claims.get("userId", String.class)).thenReturn(userId);
		when(claims.getSubject()).thenReturn(username);
	}

	@Test
	@DisplayName("a request with a valid token cookie yields the username it names")
	void validTokenCookieYieldsItsUsername() {
		request.setCookies(new Cookie("Jwttoken", "valid-token"));
		stubValidTokenFor("31", "field.worker");

		ResponseEntity<String> response = authUtil.validateJwtToken(request);

		assertEquals(HttpStatus.OK, response.getStatusCode());
		assertEquals("field.worker", response.getBody());
	}

	@Test
	@DisplayName("a request with no token cookie is unauthorized")
	void requestWithNoTokenCookieIsUnauthorized() {
		ResponseEntity<String> response = authUtil.validateJwtToken(request);

		assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
		assertTrue(response.getBody().contains("JWT Token is not set"));
	}

	@Test
	@DisplayName("a request with an invalid token is unauthorized")
	void requestWithAnInvalidTokenIsUnauthorized() {
		request.setCookies(new Cookie("Jwttoken", "invalid-token"));
		when(jwtUtil.validateToken(anyString())).thenReturn(null);

		ResponseEntity<String> response = authUtil.validateJwtToken(request);

		assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
		assertTrue(response.getBody().contains("Invalid JWT Token"));
	}

	@Test
	@DisplayName("a token naming no user is unauthorized rather than treated as anonymous")
	void tokenNamingNoUserIsUnauthorized() {
		request.setCookies(new Cookie("Jwttoken", "valid-token"));
		when(jwtUtil.validateToken(anyString())).thenReturn(claims);
		when(claims.getSubject()).thenReturn(null);

		ResponseEntity<String> response = authUtil.validateJwtToken(request);

		assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
		assertTrue(response.getBody().contains("Username is missing"));
	}

	@Test
	@DisplayName("a token naming an empty user is unauthorized")
	void tokenNamingAnEmptyUserIsUnauthorized() {
		request.setCookies(new Cookie("Jwttoken", "valid-token"));
		when(jwtUtil.validateToken(anyString())).thenReturn(claims);
		when(claims.getSubject()).thenReturn("");

		assertEquals(HttpStatus.UNAUTHORIZED, authUtil.validateJwtToken(request).getStatusCode());
	}

	@Test
	@DisplayName("a cached user satisfies the check without a database query")
	void cachedUserSatisfiesTheCheckWithoutAQuery() throws Exception {
		stubValidTokenFor("31", "field.worker");
		when(valueOperations.get("user_31")).thenReturn(new User());

		assertTrue(authUtil.validateUserIdAndJwtToken("valid-token"));

		verify(jdbcTemplate, never()).query(anyString(), any(org.springframework.jdbc.core.RowMapper.class),
				any(Object[].class));
	}

	@Test
	@DisplayName("a cache miss falls back to the database and caches the result")
	void cacheMissFallsBackToTheDatabaseAndCaches() throws Exception {
		stubValidTokenFor("31", "field.worker");
		when(valueOperations.get("user_31")).thenReturn(null);
		when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq("31")))
				.thenReturn(Collections.singletonList(new User()));

		assertTrue(authUtil.validateUserIdAndJwtToken("valid-token"));

		verify(valueOperations).set(eq("user_31"), any(), eq(30L), eq(TimeUnit.MINUTES));
	}

	@Test
	@DisplayName("a token naming a user who no longer exists is rejected")
	void tokenNamingADeletedUserIsRejected() {
		// Otherwise a token issued before the account was removed keeps working
		// until it expires.
		stubValidTokenFor("31", "field.worker");
		when(valueOperations.get("user_31")).thenReturn(null);
		when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq("31")))
				.thenReturn(Collections.emptyList());

		assertThrows(IEMRException.class, () -> authUtil.validateUserIdAndJwtToken("valid-token"));
	}

	@Test
	@DisplayName("an invalid token is rejected before any user lookup")
	void invalidTokenIsRejectedBeforeAnyUserLookup() {
		when(jwtUtil.validateToken(anyString())).thenReturn(null);

		assertThrows(IEMRException.class, () -> authUtil.validateUserIdAndJwtToken("invalid-token"));
		verify(valueOperations, never()).get(anyString());
	}

	@Test
	@DisplayName("an unreachable cache is reported rather than passed as authenticated")
	void unreachableCacheIsReported() {
		stubValidTokenFor("31", "field.worker");
		when(valueOperations.get(anyString())).thenThrow(new IllegalStateException("connection refused"));

		assertThrows(IEMRException.class, () -> authUtil.validateUserIdAndJwtToken("valid-token"));
	}

	@Test
	@DisplayName("an unreachable database is reported rather than passed as authenticated")
	void unreachableDatabaseIsReported() {
		stubValidTokenFor("31", "field.worker");
		when(valueOperations.get("user_31")).thenReturn(null);
		when(jdbcTemplate.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), eq("31")))
				.thenThrow(new IllegalStateException("connection refused"));

		assertThrows(IEMRException.class, () -> authUtil.validateUserIdAndJwtToken("valid-token"));
	}
}
