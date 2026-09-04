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
package com.iemr.common.identity.utils.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.text.ParseException;

import org.json.JSONException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.iemr.common.identity.utils.exception.IEMRException;

/**
 * Tests for the response envelope every endpoint returns.
 *
 * <p>
 * The API answers HTTP 200 for almost everything and signals failure through a
 * {@code statusCode} inside the body, so this class is what clients branch on.
 * Two behaviours matter: a JSON payload has to be embedded as structure rather
 * than as an escaped string (otherwise callers have to parse twice), and each
 * exception class has to map to the status code clients already handle.
 */
class OutputResponseTest {

	@Nested
	@DisplayName("successful responses")
	class SuccessfulResponses {

		@Test
		@DisplayName("a JSON object payload is embedded as structure, not as an escaped string")
		void jsonObjectPayloadIsEmbeddedAsStructure() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setResponse("{\"benRegId\":100200300,\"firstName\":\"Asha\"}");

			assertTrue(response.isSuccess());
			assertEquals(OutputResponse.SUCCESS, response.getStatusCode());
			assertEquals("Success", response.getStatus());
			assertTrue(response.toString().contains("\"firstName\":\"Asha\""), response.toString());
			assertFalse(response.toString().contains("\\\""), response.toString());
		}

		@Test
		@DisplayName("a JSON array payload is embedded as an array")
		void jsonArrayPayloadIsEmbeddedAsAnArray() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setResponse("[{\"benRegId\":1},{\"benRegId\":2}]");

			assertTrue(response.getData().startsWith("["), response.getData());
			assertTrue(response.isSuccess());
		}

		@Test
		@DisplayName("a plain-text payload is wrapped so the body is still valid JSON")
		void plainTextPayloadIsWrapped() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setResponse("Updated successfully");

			assertTrue(response.toString().contains("Updated successfully"));
			assertTrue(response.getData().contains("response"), response.getData());
		}

		@Test
		@DisplayName("the payload is readable back through the data accessor")
		void payloadIsReadableBackThroughTheAccessor() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setResponse("{\"count\":42}");

			assertTrue(response.getData().contains("42"));
		}

		@Test
		@DisplayName("a response with no payload reports no data rather than failing")
		void responseWithNoPayloadReportsNoData() throws JSONException {
			assertNull(new OutputResponse().getData());
		}

		@Test
		@DisplayName("the serialising variant keeps null fields so clients see the full shape")
		void serialisingVariantKeepsNullFields() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setResponse("{\"benRegId\":1}");

			assertTrue(response.toStringWithSerialization().contains("statusCode"));
		}
	}

	@Nested
	@DisplayName("error responses")
	class ErrorResponses {

		@Test
		@DisplayName("an explicit code and message are reported as given")
		void explicitCodeAndMessageAreReportedAsGiven() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(OutputResponse.GENERIC_FAILURE, "Beneficiary not found");

			assertFalse(response.isSuccess());
			assertEquals(OutputResponse.GENERIC_FAILURE, response.getStatusCode());
			assertEquals("Beneficiary not found", response.getErrorMessage());
			assertEquals("Beneficiary not found", response.getStatus());
		}

		@Test
		@DisplayName("a separate status can be reported alongside the message")
		void separateStatusCanBeReported() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(OutputResponse.PREVILAGE_FAILURE, "Not permitted", "failure");

			assertEquals("Not permitted", response.getErrorMessage());
			assertEquals("failure", response.getStatus());
		}

		@Test
		@DisplayName("a login failure is reported as a user-id failure so the UI can prompt again")
		void loginFailureIsReportedAsUserIdFailure() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(new IEMRException("Invalid login key or session is expired"));

			assertEquals(OutputResponse.USERID_FAILURE, response.getStatusCode());
			assertEquals("User login failed", response.getStatus());
			assertEquals("Invalid login key or session is expired", response.getErrorMessage());
		}

		@Test
		@DisplayName("a malformed-payload failure is reported as an object failure")
		void malformedPayloadIsReportedAsObjectFailure() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(new JSONException("not an object"));

			assertEquals(OutputResponse.OBJECT_FAILURE, response.getStatusCode());
			assertEquals("Invalid object conversion", response.getErrorMessage());
		}

		@Test
		@DisplayName("a database or coding fault is reported as a code exception, not as a bad request")
		void databaseOrCodingFaultIsReportedAsCodeException() throws JSONException {
			// These are not the caller's fault, so the message tells them to
			// retry and escalate rather than to fix their request.
			for (Throwable thrown : new Throwable[] { new SQLException("deadlock"), new ParseException("bad", 0),
					new NullPointerException("npe"), new ArrayIndexOutOfBoundsException("aioobe") }) {
				OutputResponse response = new OutputResponse();

				response.setError(thrown);

				assertEquals(OutputResponse.CODE_EXCEPTION, response.getStatusCode(),
						thrown.getClass().getSimpleName() + " should map to a code exception");
				assertTrue(response.getStatus().contains("contact your administrator"));
			}
		}

		@Test
		@DisplayName("a connectivity fault is reported as an environment exception")
		void connectivityFaultIsReportedAsEnvironmentException() throws JSONException {
			for (Throwable thrown : new Throwable[] { new IOException("timeout"),
					new ConnectException("connection refused") }) {
				OutputResponse response = new OutputResponse();

				response.setError(thrown);

				assertEquals(OutputResponse.ENVIRONMENT_EXCEPTION, response.getStatusCode(),
						thrown.getClass().getSimpleName() + " should map to an environment exception");
				assertTrue(response.getStatus().contains("connection issues"));
			}
		}

		@Test
		@DisplayName("an unrecognised fault falls back to a generic failure carrying its message")
		void unrecognisedFaultFallsBackToGenericFailure() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(new IllegalStateException("something unexpected"));

			assertEquals(OutputResponse.GENERIC_FAILURE, response.getStatusCode());
			assertEquals("something unexpected", response.getErrorMessage());
			assertTrue(response.getStatus().contains("something unexpected"));
		}

		@Test
		@DisplayName("an error response still serialises to valid JSON")
		void errorResponseStillSerialisesToValidJson() throws JSONException {
			OutputResponse response = new OutputResponse();

			response.setError(OutputResponse.CODE_EXCEPTION, "boom");

			assertNotNull(new org.json.JSONObject(response.toString()));
		}
	}
}
