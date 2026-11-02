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
package com.iemr.common.identity.utils.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.utils.config.ConfigProperties;
import com.iemr.common.identity.utils.exception.IEMRException;
import com.iemr.common.identity.utils.redis.RedisSessionException;
import com.iemr.common.identity.utils.sessionobject.SessionObject;

/**
 * Tests for login-session validation.
 *
 * <p>
 * The validator decides whether a login key is still usable, and optionally
 * whether the request comes from the IP the session was created on. IP checking
 * is configuration-gated, so both settings need covering: with it off, a
 * session used from a new address must still work (field workers roam between
 * networks), and with it on, the response must say which address holds the
 * session rather than silently succeeding.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ValidatorTest {

	@Mock
	private SessionObject session;

	private Validator validator;

	private static final String LOGIN_KEY = "session-key";

	private void configureIpValidation(boolean enabled) {
		Properties properties = new Properties();
		properties.setProperty("enableIPValidation", String.valueOf(enabled));
		ReflectionTestUtils.setField(ConfigProperties.class, "properties", properties);
		// enableIPValidation is a static field cached across instances; the
		// constructor only re-reads configuration while it is false, so reset it
		// to false rather than null (which the constructor would unbox).
		ReflectionTestUtils.setField(Validator.class, "enableIPValidation", Boolean.FALSE);
		validator = new Validator();
		validator.setSessionObject(session);
	}

	private JSONObject loginResponse(String ipAddress) throws JSONException {
		JSONObject response = new JSONObject();
		response.put("userName", "field.worker");
		response.put("loginIPAddress", ipAddress);
		return response;
	}

	@Nested
	@DisplayName("with IP validation switched off")
	class WithIpValidationOff {

		@BeforeEach
		void setUp() throws Exception {
			configureIpValidation(false);
		}

		@Test
		@DisplayName("a fresh login is stored and reported as successful")
		void freshLoginIsStoredAndReportedSuccessful() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(null);

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertEquals("login success", result.getString("sessionStatus"));
			assertEquals(LOGIN_KEY, result.getString("key"));
			verify(session).setSessionObject(anyString(), anyString());
		}

		@Test
		@DisplayName("a session already held from another address is still accepted")
		void sessionHeldFromAnotherAddressIsStillAccepted() throws Exception {
			// Field workers move between mobile networks mid-shift, so the
			// address changing is not by itself suspicious.
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.9.9.9").toString());

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertEquals("login success", result.getString("sessionStatus"));
		}

		@Test
		@DisplayName("a session lookup failure still leaves the login usable")
		void sessionLookupFailureStillLeavesTheLoginUsable() throws Exception {
			when(session.getSessionObject(LOGIN_KEY))
					.thenThrow(new RedisSessionException("Unable to fetch session object"));

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertEquals("login success", result.getString("sessionStatus"));
		}

		@Test
		@DisplayName("a failure storing the session is reported as a failed session creation")
		void failureStoringTheSessionIsReported() throws Exception {
			when(session.setSessionObject(anyString(), anyString()))
					.thenThrow(new RedisSessionException("connection refused"));

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertEquals("session creation failed", result.getString("sessionStatus"));
		}

		@Test
		@DisplayName("a known login key passes the existence check")
		void knownLoginKeyPassesTheExistenceCheck() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.1.2.3").toString());

			validator.checkKeyExists(LOGIN_KEY, "10.1.2.3");
		}

		@Test
		@DisplayName("a login key from a different address still passes when IP validation is off")
		void loginKeyFromADifferentAddressStillPasses() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.9.9.9").toString());

			validator.checkKeyExists(LOGIN_KEY, "10.1.2.3");
		}

		@Test
		@DisplayName("an expired login key is rejected")
		void expiredLoginKeyIsRejected() throws Exception {
			when(session.getSessionObject(LOGIN_KEY))
					.thenThrow(new RedisSessionException("Unable to fetch session object"));

			IEMRException thrown = assertThrows(IEMRException.class,
					() -> validator.checkKeyExists(LOGIN_KEY, "10.1.2.3"));

			assertTrue(thrown.getMessage().contains("Invalid login key or session is expired"));
		}

		@Test
		@DisplayName("a session that is not valid JSON is rejected")
		void sessionThatIsNotValidJsonIsRejected() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn("not json at all {");

			assertThrows(IEMRException.class, () -> validator.checkKeyExists(LOGIN_KEY, "10.1.2.3"));
		}

		@Test
		@DisplayName("the stored session is readable through the validator")
		void storedSessionIsReadableThroughTheValidator() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn("{\"userName\":\"field.worker\"}");

			assertEquals("{\"userName\":\"field.worker\"}", validator.getSessionObject(LOGIN_KEY));
		}
	}

	@Nested
	@DisplayName("with IP validation switched on")
	class WithIpValidationOn {

		@BeforeEach
		void setUp() throws Exception {
			configureIpValidation(true);
		}

		@Test
		@DisplayName("a login from the address that holds the session is accepted")
		void loginFromTheHoldingAddressIsAccepted() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.1.2.3").toString());

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertEquals("login success", result.getString("sessionStatus"));
		}

		@Test
		@DisplayName("a login from a new address reports where the session is held instead of the login payload")
		void loginFromANewAddressReportsWhereTheSessionIsHeld() throws Exception {
			// The response is replaced with a bare object, so the caller gets no
			// user details - only the reason and the holding address.
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.9.9.9").toString());

			JSONObject result = validator.updateCacheObj(loginResponse("10.1.2.3"), LOGIN_KEY, "ipKey");

			assertTrue(result.getString("sessionStatus").contains("10.9.9.9"));
			assertEquals(LOGIN_KEY, result.getString("key"));
			org.junit.jupiter.api.Assertions.assertFalse(result.has("userName"));
			verify(session, org.mockito.Mockito.never()).setSessionObject(anyString(), anyString());
		}

		@Test
		@DisplayName("a login key used from a different address is rejected")
		void loginKeyUsedFromADifferentAddressIsRejected() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.9.9.9").toString());

			assertThrows(IEMRException.class, () -> validator.checkKeyExists(LOGIN_KEY, "10.1.2.3"));
		}

		@Test
		@DisplayName("a login key used from the address that holds it is accepted")
		void loginKeyUsedFromTheHoldingAddressIsAccepted() throws Exception {
			when(session.getSessionObject(LOGIN_KEY)).thenReturn(loginResponse("10.1.2.3").toString());

			validator.checkKeyExists(LOGIN_KEY, "10.1.2.3");
		}
	}
}
