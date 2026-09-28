package com.iemr.common.identity.controller.rmnch;

import static com.iemr.common.identity.TestJson.assertFailure;
import static com.iemr.common.identity.TestJson.assertSuccess;
import static com.iemr.common.identity.TestJson.parse;
import static com.iemr.common.identity.TestJson.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.iemr.common.identity.service.rmnch.RmnchDataSyncService;

@ExtendWith(MockitoExtension.class)
class RMNCHMobileAppControllerTest {

	@Mock
	private RmnchDataSyncService rmnchDataSyncService;

	@InjectMocks
	private RMNCHMobileAppController controller;

	private static final String AUTH = "Bearer token";

	@Test
	@DisplayName("syncDataToAmrit returns service payload on success")
	void syncDataToAmritSuccess() throws Exception {
		when(rmnchDataSyncService.syncDataToAmrit("{\"a\":1}")).thenReturn("synced");

		assertSuccess(controller.syncDataToAmrit("{\"a\":1}"), "synced");
	}

	@Test
	@DisplayName("syncDataToAmrit reports null request as an error")
	void syncDataToAmritNullRequest() throws Exception {
		assertFailure(controller.syncDataToAmrit(null), "Invalid/NULL request obj");
	}

	@Test
	@DisplayName("syncDataToAmrit wraps service failure")
	void syncDataToAmritFailure() throws Exception {
		when(rmnchDataSyncService.syncDataToAmrit(anyString())).thenThrow(new RuntimeException("sync boom"));

		assertFailure(controller.syncDataToAmrit("{}"), "Error in RMNCH mobile data sync");
	}

	@Test
	@DisplayName("syncDataToAmritHwc returns 200 with the service result")
	void syncDataToAmritHwcSuccess() {
		String request = "{\"benficieryid\":11,\"benRegId\":22}";
		when(rmnchDataSyncService.saveBeneficiaryDetailsAfterRegistration(11L, 22L, request)).thenReturn("saved");

		ResponseEntity<?> response = controller.syncDataToAmritHwc(request);

		assertEquals(HttpStatus.OK, response.getStatusCode());
		assertEquals("saved", response.getBody());
		verify(rmnchDataSyncService).saveBeneficiaryDetailsAfterRegistration(11L, 22L, request);
	}

	@Test
	@DisplayName("syncDataToAmritHwc rejects a null request body")
	void syncDataToAmritHwcNullRequest() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc(null);

		assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
		assertEquals("Invalid/NULL request obj", response.getBody());
	}

	@Test
	@DisplayName("syncDataToAmritHwc rejects an empty request body")
	void syncDataToAmritHwcEmptyRequest() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc("");

		assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
	}

	@Test
	@DisplayName("syncDataToAmritHwc rejects a request missing benficieryid")
	void syncDataToAmritHwcMissingBeneficiaryId() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc("{\"benRegId\":22}");

		assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
		assertEquals("beneficiaryID or beneficiaryRegID is missing", response.getBody());
	}

	@Test
	@DisplayName("syncDataToAmritHwc treats an explicit JSON null id as missing")
	void syncDataToAmritHwcJsonNullBeneficiaryId() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc("{\"benficieryid\":null,\"benRegId\":22}");

		assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
	}

	@Test
	@DisplayName("syncDataToAmritHwc rejects a request missing benRegId")
	void syncDataToAmritHwcMissingRegId() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc("{\"benficieryid\":11}");

		assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
	}

	@Test
	@DisplayName("syncDataToAmritHwc returns 500 when the service throws")
	void syncDataToAmritHwcServiceFailure() {
		when(rmnchDataSyncService.saveBeneficiaryDetailsAfterRegistration(anyLong(), anyLong(), anyString()))
				.thenThrow(new RuntimeException("hwc boom"));

		ResponseEntity<?> response = controller.syncDataToAmritHwc("{\"benficieryid\":11,\"benRegId\":22}");

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		assertTrue(String.valueOf(response.getBody()).contains("hwc boom"));
	}

	@Test
	@DisplayName("syncDataToAmritHwc returns 500 on malformed JSON")
	void syncDataToAmritHwcMalformedJson() {
		ResponseEntity<?> response = controller.syncDataToAmritHwc("not-json{");

		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
	}

	@Test
	@DisplayName("getBeneficiaryData returns service payload on success")
	void getBeneficiaryDataSuccess() throws Exception {
		when(rmnchDataSyncService.getBenData("{\"villageID\":1}", AUTH)).thenReturn("[{\"benId\":1}]");

		assertSuccess(controller.getBeneficiaryData("{\"villageID\":1}", AUTH), "benId");
	}

	@Test
	@DisplayName("getBeneficiaryData reports no record when the service returns null")
	void getBeneficiaryDataNoRecord() throws Exception {
		when(rmnchDataSyncService.getBenData(anyString(), eq(AUTH))).thenReturn(null);

		assertFailure(controller.getBeneficiaryData("{}", AUTH), "No record found");
	}

	@Test
	@DisplayName("getBeneficiaryData reports null request as an error")
	void getBeneficiaryDataNullRequest() {
		assertFailure(controller.getBeneficiaryData(null, AUTH), "Invalid/NULL request obj");
	}

	@Test
	@DisplayName("getBeneficiaryData wraps service failure")
	void getBeneficiaryDataFailure() throws Exception {
		when(rmnchDataSyncService.getBenData(anyString(), eq(AUTH))).thenThrow(new RuntimeException("village boom"));

		assertFailure(controller.getBeneficiaryData("{}", AUTH), "Error in get data");
	}

	@Test
	@DisplayName("getBeneficiaryDataByAsha returns service payload on success")
	void getBeneficiaryDataByAshaSuccess() throws Exception {
		when(rmnchDataSyncService.getBenDataByAsha("{\"AshaId\":1}", AUTH)).thenReturn("[{\"benId\":2}]");

		assertSuccess(controller.getBeneficiaryDataByAsha("{\"AshaId\":1}", AUTH), "benId");
	}

	@Test
	@DisplayName("getBeneficiaryDataByAsha reports no record when the service returns null")
	void getBeneficiaryDataByAshaNoRecord() throws Exception {
		when(rmnchDataSyncService.getBenDataByAsha(anyString(), eq(AUTH))).thenReturn(null);

		assertFailure(controller.getBeneficiaryDataByAsha("{}", AUTH), "No record found");
	}

	@Test
	@DisplayName("getBeneficiaryDataByAsha reports null request as an error")
	void getBeneficiaryDataByAshaNullRequest() {
		assertFailure(controller.getBeneficiaryDataByAsha(null, AUTH), "Invalid/NULL request obj");
	}

	@Test
	@DisplayName("getBeneficiaryDataByAsha wraps service failure")
	void getBeneficiaryDataByAshaFailure() throws Exception {
		when(rmnchDataSyncService.getBenDataByAsha(anyString(), eq(AUTH))).thenThrow(new RuntimeException("asha boom"));

		assertFailure(controller.getBeneficiaryDataByAsha("{}", AUTH), "Error in get data");
	}
}
