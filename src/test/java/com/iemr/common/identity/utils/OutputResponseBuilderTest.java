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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the builder the identity endpoints use to assemble their response
 * envelope.
 *
 * <p>
 * The envelope carries the calling method and object type alongside the data so
 * that a client-reported failure can be traced back to an endpoint from the
 * response body alone. These tests pin the field names, because renaming one is
 * a silent break for every consumer.
 */
class OutputResponseBuilderTest {

	@Test
	@DisplayName("a built response carries the data, the status and the calling method")
	void builtResponseCarriesDataStatusAndCallingMethod() throws JSONException {
		OutputResponse response = new OutputResponse.Builder().setDataJsonType("JsonObject.class").setStatusCode(200)
				.setStatusMessage("success").setDataObjectType("IdentityController")
				.setMethodName("getBeneficiariesByBenRegId").setData("[{\"benRegId\":100200300}]").build();

		JSONObject envelope = new JSONObject(response.toString()).getJSONObject("response");
		assertEquals(200, envelope.getInt("statusCode"));
		assertEquals("success", envelope.getString("statusMessage"));
		assertEquals("IdentityController", envelope.getString("dataObjectType"));
		assertEquals("getBeneficiariesByBenRegId", envelope.getString("methodName"));
		assertEquals("JsonObject.class", envelope.getString("dataJsonType"));
		assertTrue(envelope.getString("data").contains("100200300"));
	}

	@Test
	@DisplayName("a failure envelope carries the failure code the client branches on")
	void failureEnvelopeCarriesTheFailureCode() throws JSONException {
		OutputResponse response = new OutputResponse.Builder().setStatusCode(5000).setStatusMessage("failure")
				.setData("\"error in beneficiary search\"").setMethodName("").setDataObjectType("IdentityController")
				.setDataJsonType("JsonObject.class").build();

		JSONObject envelope = new JSONObject(response.toString()).getJSONObject("response");
		assertEquals(5000, envelope.getInt("statusCode"));
		assertEquals("failure", envelope.getString("statusMessage"));
	}

	@Test
	@DisplayName("a long status message is carried without truncation")
	void longStatusMessageIsCarriedWithoutTruncation() throws JSONException {
		String message = "error in beneficiary advance search : could not extract ResultSet";

		OutputResponse response = new OutputResponse.Builder().setStatusCode(5000).setStatusMessage("failure")
				.setStatusMessageLong(message).setData("{}").setMethodName("").setDataObjectType("")
				.setDataJsonType("").build();

		assertEquals(message,
				new JSONObject(response.toString()).getJSONObject("response").getString("statusMessageLong"));
	}

	@Test
	@DisplayName("an envelope built with nothing set still serialises to valid JSON")
	void envelopeBuiltWithNothingSetStillSerialises() throws JSONException {
		OutputResponse response = new OutputResponse.Builder().build();

		assertTrue(new JSONObject(response.toString()).has("response"));
	}
}
