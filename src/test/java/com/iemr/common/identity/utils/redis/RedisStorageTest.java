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
package com.iemr.common.identity.utils.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for the session store behind login sessions.
 *
 * <p>
 * Every entry is written with an expiry so an abandoned session cannot outlive
 * its TTL, and a read refreshes that expiry so an active agent is not logged out
 * mid-call. The distinction that matters is between "absent" and "present": a
 * missing session must raise {@link RedisSessionException} so the caller
 * re-authenticates, rather than returning null for the caller to misread as an
 * empty-but-valid session.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisStorageTest {

	@Mock
	private LettuceConnectionFactory connectionFactory;
	@Mock
	private RedisConnection connection;

	private RedisStorage storage;

	private static final String KEY = "session-key";
	private static final int TTL_SECONDS = 1800;

	@BeforeEach
	void setUp() {
		storage = new RedisStorage();
		ReflectionTestUtils.setField(storage, "connection", connectionFactory);
		when(connectionFactory.getConnection()).thenReturn(connection);
	}

	private byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	@Test
	@DisplayName("a new session is written with an expiry")
	void newSessionIsWrittenWithAnExpiry() throws Exception {
		when(connection.get(bytes(KEY))).thenReturn(null);

		assertEquals(KEY, storage.setObject(KEY, "{\"userName\":\"field.worker\"}", TTL_SECONDS));

		verify(connection).set(eq(bytes(KEY)), eq(bytes("{\"userName\":\"field.worker\"}")),
				eq(Expiration.seconds(TTL_SECONDS)), eq(SetOption.UPSERT));
	}

	@Test
	@DisplayName("an existing session is not overwritten by a fresh login")
	void existingSessionIsNotOverwritten() throws Exception {
		// A concurrent login for the same key keeps the session already in
		// flight rather than resetting it.
		when(connection.get(bytes(KEY))).thenReturn(bytes("{\"userName\":\"field.worker\"}"));

		assertEquals(KEY, storage.setObject(KEY, "{\"userName\":\"someone.else\"}", TTL_SECONDS));

		verify(connection, never()).set(any(), any(), any(), any());
	}

	@Test
	@DisplayName("a session held under an empty value is treated as absent and written")
	void sessionHeldUnderAnEmptyValueIsWritten() throws Exception {
		when(connection.get(bytes(KEY))).thenReturn(bytes(""));

		storage.setObject(KEY, "{\"userName\":\"field.worker\"}", TTL_SECONDS);

		verify(connection).set(any(), any(), any(), any());
	}

	@Test
	@DisplayName("reading a session returns it and refreshes its expiry")
	void readingASessionRefreshesItsExpiry() throws Exception {
		when(connection.get(bytes(KEY))).thenReturn(bytes("{\"userName\":\"field.worker\"}"));

		assertEquals("{\"userName\":\"field.worker\"}", storage.getObject(KEY, true, TTL_SECONDS));

		verify(connection).expire(bytes(KEY), TTL_SECONDS);
	}

	@Test
	@DisplayName("reading an absent session is reported so the caller re-authenticates")
	void readingAnAbsentSessionIsReported() {
		when(connection.get(bytes(KEY))).thenReturn(null);

		assertThrows(RedisSessionException.class, () -> storage.getObject(KEY, true, TTL_SECONDS));
	}

	@Test
	@DisplayName("reading a blank session is reported as absent")
	void readingABlankSessionIsReportedAsAbsent() {
		when(connection.get(bytes(KEY))).thenReturn(bytes("   "));

		assertThrows(RedisSessionException.class, () -> storage.getObject(KEY, true, TTL_SECONDS));
	}

	@Test
	@DisplayName("updating an existing session rewrites its value and expiry")
	void updatingAnExistingSessionRewritesItsValueAndExpiry() throws Exception {
		when(connection.get(bytes(KEY))).thenReturn(bytes("{\"userName\":\"field.worker\"}"));

		assertEquals(KEY, storage.updateObject(KEY, "{\"userName\":\"field.worker\",\"role\":\"nurse\"}", true,
				TTL_SECONDS));

		verify(connection).set(eq(bytes(KEY)), eq(bytes("{\"userName\":\"field.worker\",\"role\":\"nurse\"}")),
				eq(Expiration.seconds(TTL_SECONDS)), eq(SetOption.UPSERT));
	}

	@Test
	@DisplayName("updating a session that has already expired is reported rather than recreating it")
	void updatingAnExpiredSessionIsReported() {
		// Recreating it would silently revive a session the user was logged out
		// of.
		when(connection.get(bytes(KEY))).thenReturn(null);

		assertThrows(RedisSessionException.class,
				() -> storage.updateObject(KEY, "{\"userName\":\"field.worker\"}", true, TTL_SECONDS));
	}

	@Test
	@DisplayName("deleting a session reports how many entries were removed")
	void deletingASessionReportsHowManyWereRemoved() throws Exception {
		when(connection.del(bytes(KEY))).thenReturn(1L);

		assertEquals(1L, storage.deleteObject(KEY));
	}

	@Test
	@DisplayName("deleting a session that is not there reports zero")
	void deletingAnAbsentSessionReportsZero() throws Exception {
		when(connection.del(bytes(KEY))).thenReturn(0L);

		assertEquals(0L, storage.deleteObject(KEY));
	}
}
