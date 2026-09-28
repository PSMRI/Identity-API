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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

/**
 * Tests for JWT issuing and verification.
 *
 * <p>
 * This is the only thing standing between an unauthenticated caller and every
 * beneficiary record, so the tests are written around what must be rejected: a
 * token signed with a different key, a tampered payload, an expired token, and
 * a token whose id has been revoked. {@code validateToken} answers all of those
 * with {@code null} rather than an exception, which makes an accidental
 * "invalid means valid" inversion easy to introduce and invisible without these
 * cases.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JwtUtilTest {

	/** HMAC-SHA256 needs at least 256 bits of key material. */
	private static final String SECRET = "identity-api-test-signing-secret-key-0123456789";
	private static final long ACCESS_EXPIRY_MS = 60_000L;
	private static final long REFRESH_EXPIRY_MS = 600_000L;

	@Mock
	private TokenDenylist tokenDenylist;

	private JwtUtil jwtUtil;

	@BeforeEach
	void setUp() {
		jwtUtil = new JwtUtil();
		ReflectionTestUtils.setField(jwtUtil, "SECRET_KEY", SECRET);
		ReflectionTestUtils.setField(jwtUtil, "ACCESS_EXPIRATION_TIME", ACCESS_EXPIRY_MS);
		ReflectionTestUtils.setField(jwtUtil, "REFRESH_EXPIRATION_TIME", REFRESH_EXPIRY_MS);
		ReflectionTestUtils.setField(jwtUtil, "tokenDenylist", tokenDenylist);
		when(tokenDenylist.isTokenDenylisted(anyString())).thenReturn(false);
	}

	private SecretKey key(String secret) {
		return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
	}

	@Nested
	@DisplayName("issuing tokens")
	class IssuingTokens {

		@Test
		@DisplayName("an access token carries the username, user id and its type")
		void accessTokenCarriesItsClaims() {
			String token = jwtUtil.generateToken("field.worker", "31");

			Claims claims = jwtUtil.validateToken(token);
			assertEquals("field.worker", claims.getSubject());
			assertEquals("31", claims.get("userId", String.class));
			assertEquals("access", claims.get("token_type", String.class));
			assertNotNull(claims.getId());
			assertNotNull(claims.getIssuedAt());
		}

		@Test
		@DisplayName("a refresh token is distinguishable from an access token")
		void refreshTokenIsDistinguishable() {
			// Accepting a refresh token where an access token is expected would
			// extend a session far past its intended lifetime.
			Claims claims = jwtUtil.validateToken(jwtUtil.generateRefreshToken("field.worker", "31"));

			assertEquals("refresh", claims.get("token_type", String.class));
		}

		@Test
		@DisplayName("a refresh token outlives an access token")
		void refreshTokenOutlivesAccessToken() {
			Claims access = jwtUtil.validateToken(jwtUtil.generateToken("field.worker", "31"));
			Claims refresh = jwtUtil.validateToken(jwtUtil.generateRefreshToken("field.worker", "31"));

			assertTrue(refresh.getExpiration().after(access.getExpiration()));
			assertEquals(REFRESH_EXPIRY_MS, jwtUtil.getRefreshTokenExpiration());
		}

		@Test
		@DisplayName("every token gets its own id so it can be revoked individually")
		void everyTokenGetsItsOwnId() {
			String first = jwtUtil.getJtiFromToken(jwtUtil.generateToken("field.worker", "31"));
			String second = jwtUtil.getJtiFromToken(jwtUtil.generateToken("field.worker", "31"));

			assertNotEquals(first, second);
		}

		@ParameterizedTest
		@ValueSource(strings = { "", "   " })
		@DisplayName("a token cannot be issued without a username")
		void tokenCannotBeIssuedWithoutAUsername(String username) {
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.generateToken(username, "31"));
		}

		@Test
		@DisplayName("a token cannot be issued for a null username")
		void tokenCannotBeIssuedForNullUsername() {
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.generateToken(null, "31"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "", "   " })
		@DisplayName("a token cannot be issued without a user id")
		void tokenCannotBeIssuedWithoutAUserId(String userId) {
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.generateToken("field.worker", userId));
		}

		@Test
		@DisplayName("a token cannot be issued for a null user id")
		void tokenCannotBeIssuedForNullUserId() {
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.generateRefreshToken("field.worker", null));
		}

		@ParameterizedTest
		@ValueSource(strings = { "" })
		@DisplayName("a deployment with no configured signing secret cannot issue tokens")
		void deploymentWithNoSecretCannotIssueTokens(String secret) {
			// Falling back to a default key would make every deployment forge
			// tokens for every other one.
			ReflectionTestUtils.setField(jwtUtil, "SECRET_KEY", secret);

			assertThrows(IllegalStateException.class, () -> jwtUtil.generateToken("field.worker", "31"));
		}

		@Test
		@DisplayName("a deployment with a null signing secret cannot issue tokens")
		void deploymentWithNullSecretCannotIssueTokens() {
			ReflectionTestUtils.setField(jwtUtil, "SECRET_KEY", null);

			assertThrows(IllegalStateException.class, () -> jwtUtil.generateToken("field.worker", "31"));
		}
	}

	@Nested
	@DisplayName("verifying tokens")
	class VerifyingTokens {

		@Test
		@DisplayName("a token signed with a different key is rejected")
		void tokenSignedWithADifferentKeyIsRejected() {
			String forged = Jwts.builder().subject("field.worker").claim("userId", "31")
					.id(UUID.randomUUID().toString())
					// Same length as the real secret, so the same HMAC algorithm is
					// selected and only the key material differs.
					.signWith(key(SECRET.replace('i', 'x'))).compact();

			assertNull(jwtUtil.validateToken(forged));
		}

		@Test
		@DisplayName("an expired token is rejected")
		void expiredTokenIsRejected() {
			String expired = Jwts.builder().subject("field.worker").claim("userId", "31")
					.id(UUID.randomUUID().toString()).issuedAt(new Date(System.currentTimeMillis() - 120_000L))
					.expiration(new Date(System.currentTimeMillis() - 60_000L)).signWith(key(SECRET)).compact();

			assertNull(jwtUtil.validateToken(expired));
		}

		@Test
		@DisplayName("a revoked token is rejected even though its signature is valid")
		void revokedTokenIsRejected() {
			String token = jwtUtil.generateToken("field.worker", "31");
			when(tokenDenylist.isTokenDenylisted(anyString())).thenReturn(true);

			assertNull(jwtUtil.validateToken(token));
		}

		@ParameterizedTest
		@ValueSource(strings = { "not-a-token", "a.b.c", "", "eyJhbGciOiJIUzI1NiJ9.tampered.signature" })
		@DisplayName("a malformed token is rejected")
		void malformedTokenIsRejected(String token) {
			assertNull(jwtUtil.validateToken(token));
		}

		@Test
		@DisplayName("a null token is rejected")
		void nullTokenIsRejected() {
			assertNull(jwtUtil.validateToken(null));
		}

		@Test
		@DisplayName("an unsigned token is rejected")
		void unsignedTokenIsRejected() {
			// A "none"-algorithm token would otherwise let a caller mint their
			// own claims.
			String unsigned = Jwts.builder().subject("field.worker").claim("userId", "31").compact();

			assertNull(jwtUtil.validateToken(unsigned));
		}

		@Test
		@DisplayName("a token with no id is accepted without consulting the revocation list")
		void tokenWithNoIdSkipsTheRevocationCheck() {
			String noJti = Jwts.builder().subject("field.worker").claim("userId", "31")
					.expiration(new Date(System.currentTimeMillis() + 60_000L)).signWith(key(SECRET)).compact();

			assertNotNull(jwtUtil.validateToken(noJti));
			org.mockito.Mockito.verify(tokenDenylist, org.mockito.Mockito.never()).isTokenDenylisted(anyString());
		}
	}

	@Nested
	@DisplayName("reading claims")
	class ReadingClaims {

		@Test
		@DisplayName("the username, user id and token id are read back from a valid token")
		void claimsAreReadBackFromAValidToken() {
			String token = jwtUtil.generateToken("field.worker", "31");

			assertEquals("field.worker", jwtUtil.getUsernameFromToken(token));
			assertEquals("31", jwtUtil.getUserIdFromToken(token));
			assertNotNull(jwtUtil.getJtiFromToken(token));
			assertEquals("access", jwtUtil.getClaimFromToken(token, claims -> claims.get("token_type")));
		}

		@Test
		@DisplayName("reading claims from an invalid token is rejected rather than returning nulls")
		void readingClaimsFromAnInvalidTokenIsRejected() {
			// A silent null here would read as "no username", which callers
			// could mistake for an anonymous but valid session.
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.getAllClaimsFromToken("not-a-token"));
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.getUsernameFromToken("not-a-token"));
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.getUserIdFromToken("not-a-token"));
			assertThrows(IllegalArgumentException.class, () -> jwtUtil.getJtiFromToken("not-a-token"));
		}

		@Test
		@DisplayName("reading claims from a revoked token is rejected")
		void readingClaimsFromARevokedTokenIsRejected() {
			String token = jwtUtil.generateToken("field.worker", "31");
			when(tokenDenylist.isTokenDenylisted(anyString())).thenReturn(true);

			assertThrows(IllegalArgumentException.class, () -> jwtUtil.getAllClaimsFromToken(token));
		}
	}
}
