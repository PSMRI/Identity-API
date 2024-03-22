package com.iemr.common.identity.controller.rmnch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.iemr.common.identity.service.rmnch.RmnchDataSyncService;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
@ExtendWith(MockitoExtension.class)
class RMNCHMobileAppControllerTest {
	@InjectMocks
	RMNCHMobileAppController rmnchMobileAppController;
	@Mock
	RmnchDataSyncService rmnchDataSyncService;

	@Test
	void testSyncDataToAmrit() throws Exception {
		String req = "requestObj";
		when(rmnchDataSyncService.syncDataToAmrit(any())).thenReturn("resp");
		String syncDataToAmrit = rmnchMobileAppController.syncDataToAmrit(req);
		assertNotNull(syncDataToAmrit);
	}
	@Test
	void testSyncDataToAmritNullReq() throws Exception {
		String req = null;
		String syncDataToAmrit = rmnchMobileAppController.syncDataToAmrit(req);
		String data = getData(syncDataToAmrit,"errorMessage");
		assertEquals("Invalid/NULL request obj", data);
	}
	@Test
	void testSyncDataToAmritException() throws Exception {
		String req = "requestObj";
		when(rmnchDataSyncService.syncDataToAmrit(any())).thenThrow(Exception.class);
		String syncDataToAmrit = rmnchMobileAppController.syncDataToAmrit(req);
		String data = getData(syncDataToAmrit,"errorMessage");
		assertTrue(data.contains("Error in RMNCH mobile data sync"));
	}

	@Test
	void testGetBeneficiaryData() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenData(req, auth)).thenReturn("resp");
		String beneficiaryData = rmnchMobileAppController.getBeneficiaryData(req, auth);
		assertNotNull(beneficiaryData);
	}
	@Test
	void testGetBeneficiaryDataNullResp() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenData(req, auth)).thenReturn(null);
		String beneficiaryData = rmnchMobileAppController.getBeneficiaryData(req, auth);
		String data = getData(beneficiaryData,"errorMessage");
		assertTrue(data.contains("No record found"));
	}
	@Test
	void testGetBeneficiaryDataNullReq() throws Exception {
		String req = null;
		String auth = "authorization";
		String beneficiaryData = rmnchMobileAppController.getBeneficiaryData(req, auth);
		String data = getData(beneficiaryData,"errorMessage");
		assertEquals("Invalid/NULL request obj", data);
	}
	@Test
	void testGetBeneficiaryDataException() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenData(req, auth)).thenThrow(Exception.class);
		String beneficiaryData = rmnchMobileAppController.getBeneficiaryData(req, auth);
		String data = getData(beneficiaryData,"errorMessage");
		assertTrue(data.contains("Error in get data"));
	}

	@Test
	void testGetBeneficiaryDataByAsha() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenDataByAsha(req, auth)).thenReturn("resp");
		String beneficiaryDataByAsha = rmnchMobileAppController.getBeneficiaryDataByAsha(req, auth);
		assertNotNull(beneficiaryDataByAsha);
	}
	
	@Test
	void testGetBeneficiaryDataByAshaNullResp() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenDataByAsha(req, auth)).thenReturn(null);
		String beneficiaryDataByAsha = rmnchMobileAppController.getBeneficiaryDataByAsha(req, auth);
		String data = getData(beneficiaryDataByAsha,"errorMessage");
		assertTrue(data.contains("No record found"));
	}
	@Test
	void testGetBeneficiaryDataByAshaNullReq() throws Exception {
		String req = null;
		String auth = "authorization";
		String beneficiaryDataByAsha = rmnchMobileAppController.getBeneficiaryDataByAsha(req, auth);
		String data = getData(beneficiaryDataByAsha,"errorMessage");
		assertEquals("Invalid/NULL request obj", data);
	}
	
	@Test
	void testGetBeneficiaryDataByAshaException() throws Exception {
		String req = "requestObj";
		String auth = "authorization";
		when(rmnchDataSyncService.getBenDataByAsha(req, auth)).thenThrow(Exception.class);
		String beneficiaryDataByAsha = rmnchMobileAppController.getBeneficiaryDataByAsha(req, auth);
		String data = getData(beneficiaryDataByAsha,"errorMessage");
		assertTrue(data.contains("Error in get data"));
	}
	private String getData(String resp, String status) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(resp);
		String actualresp = json.getAsString(status);
		return actualresp;
	}
}
