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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 * Tests for the outbound HTTP helper used to reach the teleconsultation and
 * FHIR services.
 *
 * <p>
 * The helper's job is header assembly: an inbound {@code Authorization} header
 * has to be forwarded on the outbound call, and a content type has to default
 * to JSON when the caller does not name one. Dropping the authorization header
 * turns an RMNCH lookup into a silent 401 that the calling code reports as "no
 * data", which is what these tests are for.
 *
 * <p>
 * {@code uploadFile} is not covered here: its multipart branch touches
 * {@code javax.ws.rs.core.MediaType}, whose static initialiser needs a JAX-RS
 * runtime delegate that this service does not package.
 */
class HttpUtilsTest {

	private RestTemplate restTemplate;
	private HttpUtils httpUtils;

	@BeforeEach
	void setUp() {
		restTemplate = mock(RestTemplate.class);
		httpUtils = new HttpUtils();
		ReflectionTestUtils.setField(httpUtils, "rest", restTemplate);
	}

	@SuppressWarnings("unchecked")
	private void stubExchange(String responseBody) {
		when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>(responseBody, HttpStatus.OK));
	}

	@SuppressWarnings("unchecked")
	private HttpEntity<String> capturedRequest() {
		ArgumentCaptor<HttpEntity<String>> captor = ArgumentCaptor.forClass(HttpEntity.class);
		org.mockito.Mockito.verify(restTemplate).exchange(anyString(), any(HttpMethod.class), captor.capture(),
				eq(String.class));
		return captor.getValue();
	}

	@Test
	@DisplayName("a plain GET returns the response body and records the status")
	void plainGetReturnsBodyAndRecordsStatus() {
		stubExchange("{\"data\":[]}");

		assertEquals("{\"data\":[]}", httpUtils.get("http://localhost:8093/healthID/getBenhealthID"));
		assertEquals(HttpStatus.OK, httpUtils.getStatus());
	}

	@Test
	@DisplayName("a GET forwards the caller's authorization header")
	void getForwardsTheAuthorizationHeader() {
		stubExchange("{}");
		Map<String, Object> header = new HashMap<>();
		header.put(HttpHeaders.AUTHORIZATION, "Bearer inbound-token");

		httpUtils.get("http://localhost:8089/ANC/getHRPStatus", header);

		assertEquals("Bearer inbound-token", capturedRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	@DisplayName("a GET defaults the content type to JSON when the caller does not name one")
	void getDefaultsContentTypeToJson() {
		stubExchange("{}");

		httpUtils.get("http://localhost:8089/ANC/getHRPStatus", new HashMap<>());

		assertEquals("application/json", capturedRequest().getHeaders().getFirst(HttpHeaders.CONTENT_TYPE));
	}

	@Test
	@DisplayName("a GET honours an explicit content type")
	void getHonoursAnExplicitContentType() {
		stubExchange("{}");
		Map<String, Object> header = new HashMap<>();
		header.put(HttpHeaders.CONTENT_TYPE, "application/xml");

		httpUtils.get("http://localhost:8089/ANC/getHRPStatus", header);

		assertEquals("application/xml", capturedRequest().getHeaders().getFirst(HttpHeaders.CONTENT_TYPE));
	}

	@Test
	@DisplayName("a plain POST sends the body as JSON")
	void plainPostSendsTheBodyAsJson() {
		stubExchange("{\"data\":{\"isHRP\":true}}");

		String response = httpUtils.post("http://localhost:8089/ANC/getHRPStatus", "{\"benRegID\":100200300}");

		assertEquals("{\"data\":{\"isHRP\":true}}", response);
		assertEquals("{\"benRegID\":100200300}", capturedRequest().getBody());
		assertEquals("application/json", capturedRequest().getHeaders().getFirst(HttpHeaders.CONTENT_TYPE));
	}

	@Test
	@DisplayName("a POST with headers forwards the authorization header along with the body")
	void postWithHeadersForwardsAuthorizationAndBody() {
		stubExchange("{}");
		Map<String, Object> header = new HashMap<>();
		header.put(HttpHeaders.AUTHORIZATION, "Bearer inbound-token");

		httpUtils.post("http://localhost:8093/healthID/getBenhealthID", "{\"beneficiaryRegID\":100200300}", header);

		assertEquals("Bearer inbound-token", capturedRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
		assertEquals("{\"beneficiaryRegID\":100200300}", capturedRequest().getBody());
	}

	@Test
	@DisplayName("a POST without an authorization header sends none rather than an empty one")
	void postWithoutAuthorizationSendsNone() {
		stubExchange("{}");

		httpUtils.post("http://localhost:8093/healthID/getBenhealthID", "{}", new HashMap<>());

		org.junit.jupiter.api.Assertions
				.assertNull(capturedRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	@DisplayName("an unreachable service propagates so the caller can degrade")
	void unreachableServicePropagates() {
		// The RMNCH service turns this into a null result and logs it; swallowing
		// it here would make that indistinguishable from an empty response.
		when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class)))
				.thenThrow(new RestClientException("connection refused"));

		assertThrows(RestClientException.class,
				() -> httpUtils.post("http://localhost:8089/ANC/getHRPStatus", "{}"));
	}

	@Test
	@DisplayName("the recorded status is readable and settable for callers that branch on it")
	void recordedStatusIsReadableAndSettable() {
		httpUtils.setStatus(HttpStatus.SERVICE_UNAVAILABLE);

		assertEquals(HttpStatus.SERVICE_UNAVAILABLE, httpUtils.getStatus());
	}
}
