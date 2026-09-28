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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.utils.exception.TokenDenylistException;

/**
 * Tests for token revocation.
 *
 * <p>
 * Revocation is stored in Redis with a TTL matching the token's own lifetime, so
 * entries expire on their own. The behaviour that matters for security is the
 * failure mode: if Redis is unreachable, a revocation check must fail loudly
 * rather than return "not revoked", because the latter would silently reinstate
 * every logged-out session.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TokenDenylistTest {

	@Mock
	private RedisTemplate<String, Object> redisTemplate;
	@Mock
	private ValueOperations<String, Object> valueOperations;

	private TokenDenylist denylist;

	private static final String JTI = "0f5c4c1e-9e46-4f66-9b3a-8ad6f0f0d0a1";
	/** The key prefix the entries are namespaced under. */
	private static final String KEY = "denied_" + JTI;

	@BeforeEach
	void setUp() {
		denylist = new TokenDenylist();
		ReflectionTestUtils.setField(denylist, "redisTemplate", redisTemplate);
		when(redisTemplate.opsForValue()).thenReturn(valueOperations);
	}

	@Test
	@DisplayName("a revoked token is stored with a TTL matching its remaining lifetime")
	void revokedTokenIsStoredWithATtl() {
		denylist.addTokenToDenylist(JTI, 60_000L);

		verify(valueOperations).set(eq(KEY), any(), eq(60_000L), eq(TimeUnit.MILLISECONDS));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "   " })
	@DisplayName("a blank token id is ignored rather than stored under a bare prefix")
	void blankTokenIdIsIgnored(String jti) {
		// Storing under the bare prefix would revoke nothing while looking like
		// it worked.
		denylist.addTokenToDenylist(jti, 60_000L);

		verify(valueOperations, never()).set(anyString(), any(), anyLong(), any());
	}

	@Test
	@DisplayName("a non-positive expiry is rejected so an entry cannot outlive its token")
	void nonPositiveExpiryIsRejected() {
		assertThrows(IllegalArgumentException.class, () -> denylist.addTokenToDenylist(JTI, 0L));
		assertThrows(IllegalArgumentException.class, () -> denylist.addTokenToDenylist(JTI, -1L));
		assertThrows(IllegalArgumentException.class, () -> denylist.addTokenToDenylist(JTI, null));
	}

	@Test
	@DisplayName("a failure to store a revocation is surfaced rather than silently dropped")
	void failureToStoreIsSurfaced() {
		// The caller is a logout; reporting success while the token stays valid
		// is the failure this prevents.
		org.mockito.Mockito.doThrow(new IllegalStateException("connection refused")).when(valueOperations)
				.set(anyString(), any(), anyLong(), any());

		assertThrows(TokenDenylistException.class, () -> denylist.addTokenToDenylist(JTI, 60_000L));
	}

	@Test
	@DisplayName("a token present in the store is reported as revoked")
	void tokenPresentInTheStoreIsRevoked() {
		when(redisTemplate.hasKey(KEY)).thenReturn(true);

		assertTrue(denylist.isTokenDenylisted(JTI));
	}

	@Test
	@DisplayName("a token absent from the store is not revoked")
	void tokenAbsentFromTheStoreIsNotRevoked() {
		when(redisTemplate.hasKey(KEY)).thenReturn(false);

		assertFalse(denylist.isTokenDenylisted(JTI));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "   " })
	@DisplayName("a blank token id is treated as not revoked without a lookup")
	void blankTokenIdIsTreatedAsNotRevoked(String jti) {
		assertFalse(denylist.isTokenDenylisted(jti));

		verify(redisTemplate, never()).hasKey(anyString());
	}

	@Test
	@DisplayName("an unreachable store fails the check rather than reporting the token as valid")
	void unreachableStoreFailsTheCheck() {
		// Answering "not revoked" here would reinstate every logged-out session
		// for as long as Redis is down.
		when(redisTemplate.hasKey(anyString())).thenThrow(new IllegalStateException("connection refused"));

		assertThrows(TokenDenylistException.class, () -> denylist.isTokenDenylisted(JTI));
	}

	@Test
	@DisplayName("a revocation can be lifted")
	void revocationCanBeLifted() {
		denylist.removeTokenFromDenylist(JTI);

		verify(redisTemplate).delete(KEY);
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "   " })
	@DisplayName("lifting a revocation for a blank token id does nothing")
	void liftingForABlankTokenIdDoesNothing(String jti) {
		denylist.removeTokenFromDenylist(jti);

		verify(redisTemplate, never()).delete(anyString());
	}

	@Test
	@DisplayName("a failure to lift a revocation is surfaced")
	void failureToLiftIsSurfaced() {
		when(redisTemplate.delete(anyString())).thenThrow(new IllegalStateException("connection refused"));

		assertThrows(TokenDenylistException.class, () -> denylist.removeTokenFromDenylist(JTI));
	}
}
