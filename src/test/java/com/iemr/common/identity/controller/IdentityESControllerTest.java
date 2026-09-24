package com.iemr.common.identity.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.iemr.common.identity.service.IdentityService;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchService;
import com.iemr.common.identity.utils.JwtUtil;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

@ExtendWith(MockitoExtension.class)
class IdentityESControllerTest {

	@Mock
	private ElasticsearchService elasticsearchService;

	@Mock
	private JwtUtil jwtUtil;

	@Mock
	private IdentityService idService;

	@InjectMocks
	private IdentityESController controller;

	private HttpServletRequest request;

	@BeforeEach
	void setUp() {
		request = mock(HttpServletRequest.class);
	}

	private void withJwtCookie(String token) {
		when(request.getCookies()).thenReturn(new Cookie[] { new Cookie("Jwttoken", token) });
	}

	// ---------- /search ----------

	@Test
	@DisplayName("search returns the Elasticsearch hits with a success envelope")
	void searchReturnsResults() {
		withJwtCookie("jwt-1");
		when(jwtUtil.getUserIdFromToken("jwt-1")).thenReturn("42");
		List<Map<String, Object>> hits = List.of(Map.of("beneficiaryId", "B1"));
		when(elasticsearchService.universalSearch("vani", 42)).thenReturn(hits);

		ResponseEntity<Map<String, Object>> response = controller.search("vani", request);

		assertEquals(HttpStatus.OK, response.getStatusCode());
		Map<String, Object> body = response.getBody();
		assertEquals(hits, body.get("data"));
		assertEquals(200, body.get("statusCode"));
		assertEquals("Success", body.get("status"));
		assertEquals("Success", body.get("errorMessage"));
	}

	@Test
	@DisplayName("search passes the userId decoded from the Jwttoken cookie")
	void searchUsesUserIdFromCookie() {
		withJwtCookie("jwt-2");
		when(jwtUtil.getUserIdFromToken("jwt-2")).thenReturn("7");
		when(elasticsearchService.universalSearch(anyString(), anyInt())).thenReturn(new ArrayList<>());

		controller.search("9876543210", request);

		verify(elasticsearchService).universalSearch("9876543210", 7);
	}

	@Test
	@DisplayName("search returns 500 when no Jwttoken cookie is present")
	void searchWithoutCookieReturnsError() {
		when(request.getCookies()).thenReturn(null);
		when(jwtUtil.getUserIdFromToken(isNull())).thenThrow(new IllegalArgumentException("Invalid or denylisted token"));

		ResponseEntity<Map<String, Object>> response = controller.search("vani", request);

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		Map<String, Object> body = response.getBody();
		assertEquals(500, body.get("statusCode"));
		assertEquals("Error", body.get("status"));
		assertEquals(Collections.emptyList(), body.get("data"));
	}

	@Test
	@DisplayName("search returns 500 when the userId claim is not numeric")
	void searchWithNonNumericUserIdReturnsError() {
		withJwtCookie("jwt-3");
		when(jwtUtil.getUserIdFromToken("jwt-3")).thenReturn("not-a-number");

		ResponseEntity<Map<String, Object>> response = controller.search("vani", request);

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
	}

	@Test
	@DisplayName("search returns 500 when Elasticsearch fails")
	void searchWhenElasticsearchFails() {
		withJwtCookie("jwt-4");
		when(jwtUtil.getUserIdFromToken("jwt-4")).thenReturn("1");
		when(elasticsearchService.universalSearch(anyString(), anyInt()))
				.thenThrow(new RuntimeException("cluster unavailable"));

		ResponseEntity<Map<String, Object>> response = controller.search("vani", request);

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		assertEquals("cluster unavailable", response.getBody().get("errorMessage"));
	}

	// ---------- /advancedSearchES ----------

	private Map<String, Object> serviceResult() {
		Map<String, Object> result = new HashMap<>();
		result.put("data", List.of(Map.of("beneficiaryId", "B9")));
		result.put("count", 1);
		result.put("source", "elasticsearch");
		return result;
	}

	@Test
	@DisplayName("advancedSearchES forwards every top-level filter to the service")
	void advancedSearchForwardsFilters() throws Exception {
		withJwtCookie("jwt-5");
		when(jwtUtil.getUserIdFromToken("jwt-5")).thenReturn("42");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		String filter = "{\"firstName\":\"Vani\",\"middleName\":\"S\",\"lastName\":\"R\",\"genderId\":2,"
				+ "\"dob\":\"1990-05-17\",\"stateId\":1,\"districtId\":2,\"blockId\":3,\"villageId\":4,"
				+ "\"fatherName\":\"F\",\"spouseName\":\"Sp\",\"maritalStatus\":\"Married\","
				+ "\"phoneNumber\":\"9876543210\",\"beneficiaryId\":\"B9\",\"healthId\":\"H1\","
				+ "\"aadharNo\":\"1234\",\"is1097\":true}";

		ResponseEntity<Map<String, Object>> response = controller.advanceSearchBeneficiariesES(filter, request);

		assertEquals(HttpStatus.OK, response.getStatusCode());
		verify(idService).advancedSearchBeneficiariesES(eq("Vani"), eq("S"), eq("R"), eq(2),
				eq(new SimpleDateFormat("yyyy-MM-dd").parse("1990-05-17")), eq(1), eq(2), eq(3), eq(4), eq("F"),
				eq("Sp"), eq("Married"), eq("9876543210"), eq("B9"), eq("H1"), eq("1234"), eq(42), isNull(), eq(true));
	}

	@Test
	@DisplayName("advancedSearchES copies data, count and source into the response")
	void advancedSearchCopiesServiceResult() throws Exception {
		withJwtCookie("jwt-6");
		when(jwtUtil.getUserIdFromToken("jwt-6")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		Map<String, Object> body = controller.advanceSearchBeneficiariesES("{\"firstName\":\"Vani\"}", request)
				.getBody();

		assertEquals(1, body.get("count"));
		assertEquals("elasticsearch", body.get("source"));
		assertEquals(200, body.get("statusCode"));
		assertEquals("Success", body.get("status"));
		assertNotNull(body.get("data"));
	}

	@Test
	@DisplayName("advancedSearchES treats absent and JSON-null fields as null filters")
	void advancedSearchWithAbsentAndNullFields() throws Exception {
		withJwtCookie("jwt-7");
		when(jwtUtil.getUserIdFromToken("jwt-7")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		controller.advanceSearchBeneficiariesES("{\"firstName\":null,\"genderId\":null,\"is1097\":null}", request);

		verify(idService).advancedSearchBeneficiariesES(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(),
				isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(),
				eq(1), isNull(), isNull());
	}

	@Test
	@DisplayName("advancedSearchES falls back to currentAddress for location ids")
	void advancedSearchReadsLocationFromCurrentAddress() throws Exception {
		withJwtCookie("jwt-8");
		when(jwtUtil.getUserIdFromToken("jwt-8")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		controller.advanceSearchBeneficiariesES(
				"{\"currentAddress\":{\"stateId\":11,\"districtId\":22,\"blockId\":33,\"villageId\":44}}", request);

		verify(idService).advancedSearchBeneficiariesES(isNull(), isNull(), isNull(), isNull(), isNull(), eq(11),
				eq(22), eq(33), eq(44), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), eq(1),
				isNull(), isNull());
	}

	@Test
	@DisplayName("advancedSearchES falls back to permanentAddress when currentAddress lacks the id")
	void advancedSearchReadsLocationFromPermanentAddress() throws Exception {
		withJwtCookie("jwt-9");
		when(jwtUtil.getUserIdFromToken("jwt-9")).thenReturn("1");
		ArgumentCaptor<Integer> stateCaptor = ArgumentCaptor.forClass(Integer.class);
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		controller.advanceSearchBeneficiariesES(
				"{\"currentAddress\":{\"districtId\":2},\"permanentAddress\":{\"stateId\":99}}", request);

		verify(idService).advancedSearchBeneficiariesES(isNull(), isNull(), isNull(), isNull(), isNull(),
				stateCaptor.capture(), eq(2), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(),
				isNull(), isNull(), eq(1), isNull(), isNull());
		assertEquals(99, stateCaptor.getValue());
	}

	@Test
	@DisplayName("advancedSearchES prefers a top-level location id over a nested one")
	void advancedSearchPrefersTopLevelLocationId() throws Exception {
		withJwtCookie("jwt-10");
		when(jwtUtil.getUserIdFromToken("jwt-10")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		controller.advanceSearchBeneficiariesES("{\"stateId\":5,\"currentAddress\":{\"stateId\":77}}", request);

		verify(idService).advancedSearchBeneficiariesES(isNull(), isNull(), isNull(), isNull(), isNull(), eq(5),
				isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(),
				eq(1), isNull(), isNull());
	}

	@Test
	@DisplayName("advancedSearchES ignores an unparsable dob instead of failing")
	void advancedSearchWithInvalidDob() throws Exception {
		withJwtCookie("jwt-11");
		when(jwtUtil.getUserIdFromToken("jwt-11")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(serviceResult());

		ResponseEntity<Map<String, Object>> response = controller
				.advanceSearchBeneficiariesES("{\"dob\":\"not-a-date\"}", request);

		assertEquals(HttpStatus.OK, response.getStatusCode());
		ArgumentCaptor<Date> dobCaptor = ArgumentCaptor.forClass(Date.class);
		verify(idService).advancedSearchBeneficiariesES(isNull(), isNull(), isNull(), isNull(), dobCaptor.capture(),
				isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull(),
				isNull(), eq(1), isNull(), isNull());
		assertNull(dobCaptor.getValue());
	}

	@Test
	@DisplayName("advancedSearchES returns a 500 envelope when the search filter is not JSON")
	void advancedSearchWithMalformedFilter() {
		ResponseEntity<Map<String, Object>> response = controller.advanceSearchBeneficiariesES("not-json{", request);

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		Map<String, Object> body = response.getBody();
		assertEquals(500, body.get("statusCode"));
		assertEquals("error", body.get("source"));
		assertEquals(0, body.get("count"));
		assertEquals(Collections.emptyList(), body.get("data"));
	}

	@Test
	@DisplayName("advancedSearchES returns a 500 envelope when the service throws")
	void advancedSearchWhenServiceFails() throws Exception {
		withJwtCookie("jwt-12");
		when(jwtUtil.getUserIdFromToken("jwt-12")).thenReturn("1");
		when(idService.advancedSearchBeneficiariesES(any(), any(), any(), any(), any(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
				.thenThrow(new Exception("index missing"));

		ResponseEntity<Map<String, Object>> response = controller
				.advanceSearchBeneficiariesES("{\"firstName\":\"Vani\"}", request);

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		assertEquals("index missing", response.getBody().get("errorMessage"));
	}
}
