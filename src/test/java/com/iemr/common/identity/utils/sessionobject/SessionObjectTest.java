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
package com.iemr.common.identity.utils.sessionobject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.utils.config.ConfigProperties;
import com.iemr.common.identity.utils.redis.RedisSessionException;
import com.iemr.common.identity.utils.redis.RedisStorage;

/**
 * Tests for the session facade the interceptor and validator go through.
 *
 * <p>
 * Besides storing the session under its key, it maintains a second entry keyed
 * on the lower-cased username pointing back at the session key. That is what
 * lets a new login find and displace a user's previous session, and it is
 * derived from the session JSON, so a payload without a {@code userName} has to
 * leave the primary write intact rather than fail the login.
 *
 * <p>
 * The expiry-extension flag it passes through resolves to {@code false} - see
 * {@code ConfigPropertiesTest#extendExpiryFlagReadsTheWrongProperty} for why.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SessionObjectTest {

	@Mock
	private RedisStorage objectStore;

	private SessionObject sessionObject;

	private static final String KEY = "session-key";
	private static final int TTL_SECONDS = 1800;

	@BeforeEach
	void setUp() {
		Properties properties = new Properties();
		properties.setProperty("iemr.session.expiry.time", String.valueOf(TTL_SECONDS));
		properties.setProperty("iemr.extend.expiry.time", "true");
		ReflectionTestUtils.setField(ConfigProperties.class, "properties", properties);
		ReflectionTestUtils.setField(ConfigProperties.class, "sessionExpiryTime", null);
		ReflectionTestUtils.setField(ConfigProperties.class, "extendExpiryTime", null);

		sessionObject = new SessionObject();
		sessionObject.setObjectStore(objectStore);
	}

	@Test
	@DisplayName("a session is read from the store with the configured expiry")
	void sessionIsReadWithTheConfiguredExpiry() throws Exception {
		when(objectStore.getObject(KEY, false, TTL_SECONDS)).thenReturn("{\"userName\":\"field.worker\"}");

		assertEquals("{\"userName\":\"field.worker\"}", sessionObject.getSessionObject(KEY));
	}

	@Test
	@DisplayName("an absent session propagates so the caller re-authenticates")
	void absentSessionPropagates() throws Exception {
		when(objectStore.getObject(anyString(), anyBoolean(), anyInt()))
				.thenThrow(new RedisSessionException("Unable to fetch session object from Redis server"));

		assertThrows(RedisSessionException.class, () -> sessionObject.getSessionObject(KEY));
	}

	@Test
	@DisplayName("storing a session also indexes it under the lower-cased username")
	void storingASessionIndexesItUnderTheUsername() throws Exception {
		// The username index is how a fresh login finds and displaces the
		// previous session for the same user.
		when(objectStore.setObject(eq(KEY), anyString(), anyInt())).thenReturn(KEY);

		assertEquals(KEY, sessionObject.setSessionObject(KEY, "{\"userName\":\"  Field.Worker  \"}"));

		verify(objectStore).updateObject("field.worker", KEY, false, TTL_SECONDS);
		verify(objectStore).setObject(eq(KEY), anyString(), eq(TTL_SECONDS));
	}

	@Test
	@DisplayName("a session payload with no username is still stored")
	void sessionWithNoUsernameIsStillStored() throws Exception {
		when(objectStore.setObject(eq(KEY), anyString(), anyInt())).thenReturn(KEY);

		assertEquals(KEY, sessionObject.setSessionObject(KEY, "{\"role\":\"nurse\"}"));

		verify(objectStore, never()).updateObject(eq("field.worker"), anyString(), anyBoolean(), anyInt());
	}

	@Test
	@DisplayName("an unparseable session payload is still stored under its key")
	void unparseableSessionPayloadIsStillStored() throws Exception {
		when(objectStore.setObject(eq(KEY), anyString(), anyInt())).thenReturn(KEY);

		assertEquals(KEY, sessionObject.setSessionObject(KEY, "not json at all {"));
	}

	@Test
	@DisplayName("a failure indexing the username does not stop the session being stored")
	void failureIndexingTheUsernameDoesNotStopTheStore() throws Exception {
		when(objectStore.updateObject(eq("field.worker"), anyString(), anyBoolean(), anyInt()))
				.thenThrow(new RedisSessionException("no such key"));
		when(objectStore.setObject(eq(KEY), anyString(), anyInt())).thenReturn(KEY);

		assertEquals(KEY, sessionObject.setSessionObject(KEY, "{\"userName\":\"field.worker\"}"));
	}

	@Test
	@DisplayName("updating a session refreshes both the session and its username index")
	void updatingASessionRefreshesBothEntries() throws Exception {
		when(objectStore.updateObject(eq(KEY), anyString(), anyBoolean(), anyInt())).thenReturn(KEY);

		assertEquals(KEY, sessionObject.updateSessionObject(KEY, "{\"userName\":\"field.worker\"}"));

		verify(objectStore).updateObject("field.worker", KEY, false, TTL_SECONDS);
		verify(objectStore).updateObject(eq(KEY), anyString(), eq(false), eq(TTL_SECONDS));
	}

	@Test
	@DisplayName("deleting a session removes it from the store")
	void deletingASessionRemovesIt() throws Exception {
		when(objectStore.deleteObject(KEY)).thenReturn(1L);

		sessionObject.deleteSessionObject(KEY);

		verify(objectStore).deleteObject(KEY);
	}
}
