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
package com.iemr.common.identity.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.iemr.common.identity.domain.Address;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.BenIdentityDTO;
import com.iemr.common.identity.dto.BenServiceDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.IdentityEditDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.dto.ReserveIdentityDTO;
import com.iemr.common.identity.dto.SearchSyncDTO;
import com.iemr.common.identity.exception.MissingMandatoryFieldsException;
import com.iemr.common.identity.service.IdentityService;

import jakarta.persistence.NoResultException;
import jakarta.persistence.QueryTimeoutException;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;


@ExtendWith(MockitoExtension.class)
class IdentityControllerTest {
	@InjectMocks
	IdentityController identityController;
	@Mock
	IdentityService svc;

	@Test
	void testGetBeneficiaries() throws NoResultException, QueryTimeoutException, Exception {
		IdentitySearchDTO searchParams = new IdentitySearchDTO();
		
		BeneficiariesDTO a1 = new BeneficiariesDTO();
		makeBeneficiariesDTO(a1);
		List<BeneficiariesDTO> bdList = Lists.newArrayList();
		a1.setBenId(new BigInteger("1"));
		bdList.add(a1);
		
		String req = new Gson().toJson(searchParams);
		//when(svc.getBeneficiaries(searchParams)).thenReturn(any()).thenReturn(bdList);
		String resp = identityController.getBeneficiaries(req);
		Assertions.assertNotNull(resp);

	}

	private void makeBeneficiariesDTO(BeneficiariesDTO dto) {
		dto.setAbhaDetails(null);
		dto.setAccountNo(null);
		dto.setAgeAtMarriage(null);
		dto.setBankName(null);
		dto.setBenAccountID(null);
		dto.setBeneficiaryAge(null);
		dto.setBeneficiaryDetails(null);
		List<BenFamilyDTO> tags = new ArrayList<>();
		BenFamilyDTO benFamilyDTO = new BenFamilyDTO();
		benFamilyDTO.setAssociatedBenRegId(null);
		benFamilyDTO.setBenFamilyMapId(null);
		benFamilyDTO.setCreatedBy(null);
		benFamilyDTO.setCreatedDate(null);
		benFamilyDTO.setDeleted(null);
		benFamilyDTO.setIsEmergencyContact(true);
		benFamilyDTO.setLastModDate(null);
		benFamilyDTO.setModifiedBy(null);
		benFamilyDTO.setParkingPlaceID(null);
		benFamilyDTO.setRelationshipID(null);
		benFamilyDTO.setRelationshipToSelf(null);
		benFamilyDTO.setVanID(null);
		
		tags.add(benFamilyDTO);
		dto.setBeneficiaryFamilyTags(tags);
		List<BenIdentityDTO> identity = new ArrayList<>();
		BenIdentityDTO benIdentityDTO= new BenIdentityDTO();
		benIdentityDTO.toString();
		identity.add(benIdentityDTO);
		dto.setBeneficiaryIdentites(identity);
		
		List<BenServiceDTO> service = new ArrayList<>();
		BenServiceDTO benServiceDTO= new BenServiceDTO();
		benServiceDTO.toString();
		service.add(benServiceDTO);
		dto.setBeneficiaryServiceMap(service);
		dto.setBenId(null);
		dto.setBenMapId(null);
		dto.setBenRegId(null);
		dto.setBranchName(null);
		dto.setContacts(null);
		dto.setCreatedBy(null);
		dto.setCreatedDate(null);
		Address address = new Address();
		address.toString();
		address.equals(address);
		dto.setCurrentAddress(null);
		dto.setDeleted(null);
		dto.setEmail(null);
		dto.setEmergencyAddress(address);
		dto.setEmergencyContactNum(null);
		dto.setEmergencyContactTyp(null);
		dto.setIfscCode(null);
		dto.setIncomeStatus(null);
		dto.setIsHIVPos(null);
		dto.setLastModDate(null);
		dto.setLiteracyStatus(null);
		dto.setMarriageDate(null);
		dto.setModifiedBy(null);
		dto.setMonthlyFamilyIncome(null);
		dto.setMotherName(null);
		dto.setOccupation(null);
		dto.setOccupationId(null);
		dto.setPermanentAddress(address);
		dto.setPreferredEmailId(null);
		dto.setPreferredPhoneNum(null);
		dto.setPreferredPhoneTyp(null);
		dto.setPreferredSMSPhoneNum(null);
		dto.setPreferredSMSPhoneTyp(null);
		dto.setReligion(null);
		dto.setReligionId(null);
		dto.setSourceOfInformation(null);
		dto.toString();
		BeneficiariesDTO beneficiariesDTO = new BeneficiariesDTO();
		dto.equals(beneficiariesDTO);

	}

	@Test
	void testGetBeneficiariesByBeneficiaryRegId() {
		String resp = identityController.getBeneficiariesByBeneficiaryRegId("123");
		Assertions.assertNotNull(resp);
	}
	
	@Test
	void testGetBeneficiariesByBeneficiaryRegIdCatchBlock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.getBeneficiariesByBenRegId(any())).thenThrow(NoResultException.class);
		String resp = identityController.getBeneficiariesByBeneficiaryRegId("123");
		Assertions.assertNotNull(resp);
	}

	@Test
	void testGetBeneficiariesByBeneficiaryId() {
		String resp = identityController.getBeneficiariesByBeneficiaryId("987");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testGetBeneficiariesByBeneficiaryIdCatchBlock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.getBeneficiariesByBenId(any())).thenThrow(NoResultException.class);
		String resp = identityController.getBeneficiariesByBeneficiaryId("987");
		Assertions.assertNotNull(resp);
	}

	@Test
	void testGetBeneficiariesByPhoneNum() {
		String resp = identityController.getBeneficiariesByPhoneNum("9988776655");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testGetBeneficiariesByPhoneNumCatchblock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.getBeneficiariesByPhoneNum(any())).thenThrow(NoResultException.class);
		String resp = identityController.getBeneficiariesByPhoneNum("9988776655");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testSearhBeneficiaryByABHAAddress() {
		String resp = identityController.searhBeneficiaryByABHAAddress("9876");
		Assertions.assertNotNull(resp);
	}

	@Test
	void testSearhBeneficiaryByABHAAddressCatchblock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.getBeneficiaryByHealthIDAbhaAddress(any())).thenThrow(NoResultException.class);
		String resp = identityController.searhBeneficiaryByABHAAddress("9876");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testSearhBeneficiaryByABHAIdNo() {
		String resp = identityController.searhBeneficiaryByABHAIdNo("9876");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testSearhBeneficiaryByABHAIdNoCatchblock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.getBeneficiaryByHealthIDNoAbhaIdNo(any())).thenThrow(NoResultException.class);
		String resp = identityController.searhBeneficiaryByABHAIdNo("9876");
		Assertions.assertNotNull(resp);
	}

	@Test
	void testSearhBeneficiaryByFamilyId() {
		String resp = identityController.searhBeneficiaryByFamilyId("9876");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testSearhBeneficiaryByFamilyIdCatchblock() throws NoResultException, QueryTimeoutException, Exception {
		when(svc.searhBeneficiaryByFamilyId(any())).thenThrow(NoResultException.class);
		String resp = identityController.searhBeneficiaryByFamilyId("9876");
		Assertions.assertNotNull(resp);
	}

	@Test
	void testSearchBeneficiaryByBlockIdAndLastModDate() {
		BeneficiariesDTO a1 = new BeneficiariesDTO();
		List<BeneficiariesDTO> bdList = Lists.newArrayList();
		a1.setBenId(new BigInteger("1"));
		bdList.add(a1);
		SearchSyncDTO searchSyncDTO = new SearchSyncDTO();
		String req = new Gson().toJson(searchSyncDTO);
		//when(svc.searchBeneficiaryByBlockIdAndLastModifyDate(any(), any())).thenReturn(bdList);

		String resp = identityController.searchBeneficiaryByVillageIdAndLastModDate(req);

		Assertions.assertNotNull(resp);
	}

	

	@Test
	void testReserveIdentityEmptyIdentity() throws ParseException {
		String resp = identityController.reserveIdentity("String");
		String status = getData(resp, "statusMessage");
		Assertions.assertNotEquals("Null/Empty Identity Create Data.", status);
	}
	
	@Test
	void testReserveIdentity() throws ParseException {
		ReserveIdentityDTO reserveIdentityDTO = new ReserveIdentityDTO();
		String req = new Gson().toJson(reserveIdentityDTO);
		String resp = identityController.reserveIdentity(req);
		String status = getData(resp, "statusMessage");
		Assertions.assertEquals("success", status);
	}

	@Test
	void testUnreserveIdentityEmptyIdentity() throws ParseException {
		String resp = identityController.unreserveIdentity("String");
		String status = getData(resp, "statusMessage");
		Assertions.assertNotEquals("Null/Empty Identity Create Data.", status);
		
	}
	@Test
	void testUnreserveIdentity() throws ParseException {
		ReserveIdentityDTO reserveIdentityDTO = new ReserveIdentityDTO();
		String req = new Gson().toJson(reserveIdentityDTO);
		String resp = identityController.unreserveIdentity(req);
		String status = getData(resp, "statusMessage");
		Assertions.assertEquals("success", status);
	}

	//@Test
	void testGetJsonAsString() {
		//identityController.
	}

	@Test
	void testGetFiniteBeneficiariesCatchblockIfInvalidJSON() {
		String resp = identityController.getFiniteBeneficiaries("String");
		Assertions.assertNotNull(resp);
	}
	@Test
	void testGetFiniteBeneficiaries() throws ParseException {
		IdentitySearchDTO identitySearchDTO = new IdentitySearchDTO();
		String req = new Gson().toJson(identitySearchDTO);
		String resp = identityController.getFiniteBeneficiaries(req);
		String status = getData(resp, "statusMessage");
		Assertions.assertEquals("success", status);
	}
	
	@Test
	void testGetFiniteBeneficiariesNoResultException() throws NoResultException, QueryTimeoutException, Exception {
		IdentitySearchDTO identitySearchDTO = new IdentitySearchDTO();
		String req = new Gson().toJson(identitySearchDTO);
		when(svc.getBeneficiaries(identitySearchDTO)).thenThrow(NoResultException.class);
		String resp = identityController.getFiniteBeneficiaries(req);
		String status = getData(resp, "statusMessage");
		Assertions.assertEquals("failure", status);
	}
	@Test
	void testGetFiniteBeneficiariesQueryTimeoutException() throws NoResultException, QueryTimeoutException, Exception {
		IdentitySearchDTO identitySearchDTO = new IdentitySearchDTO();
		String req = new Gson().toJson(identitySearchDTO);
		when(svc.getBeneficiaries(identitySearchDTO)).thenThrow(QueryTimeoutException.class);
		String resp = identityController.getFiniteBeneficiaries(req);
		String status = getData(resp, "statusMessage");
		Assertions.assertEquals("failure", status);
	}
	

	@Test
	void testGetBeneficiaryImageByBenRegID() {
		String resp = identityController.getBeneficiaryImageByBenRegID("String");
		Assertions.assertNull(resp);
	}

	@Test
	void testEditIdentityEducationOrCommunity() throws ParseException {
		IdentityEditDTO identityEditDTO = new IdentityEditDTO();
		String req = new Gson().toJson(identityEditDTO);
		String resp = identityController.editIdentityEducationOrCommunity(req);
		String actualresp = getData(resp,"data");
		Assertions.assertEquals("Updated successfully", actualresp);
	}
	
	
	private String getData(String resp,String status) throws ParseException {
		JSONParser parser = new JSONParser();  
		JSONObject json = (JSONObject) parser.parse(resp); 
		JSONObject object = (JSONObject) json.get("response");
		String actualresp = object.getAsString(status);
		return actualresp;
	}

	@Test
	void testEditIdentityEducationOrCommunityCatchblock() throws MissingMandatoryFieldsException, ParseException {
		IdentityEditDTO identityEditDTO = new IdentityEditDTO();
		String req = new Gson().toJson(identityEditDTO);
		doThrow(MissingMandatoryFieldsException.class).when(svc).editIdentityEducationOrCommunity(identityEditDTO);
		svc.editIdentityEducationOrCommunity(any());
		String resp = identityController.editIdentityEducationOrCommunity(req);
		String actualresp = getData(resp,"data");
		Assertions.assertNotEquals("Updated successfully", actualresp);
	}

	@Test
	void testCheckAvailablBenIDLocalServer() {
		String resp = identityController.checkAvailablBenIDLocalServer();
		Assertions.assertNotNull(resp);
	}

	@Test
	void testSaveGeneratedBenIDToLocalServer() {
		String resp = identityController.saveGeneratedBenIDToLocalServer(null);
		Assertions.assertNotNull(resp);
	}
}
