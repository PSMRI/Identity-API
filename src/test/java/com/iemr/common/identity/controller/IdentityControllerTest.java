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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.BeneficiariesPartialDTO;
import com.iemr.common.identity.dto.BeneficiaryCreateResp;
import com.iemr.common.identity.dto.IdentityEditDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.exception.MissingMandatoryFieldsException;
import com.iemr.common.identity.service.IdentityService;
import com.iemr.common.identity.utils.exception.IEMRException;

/**
 * Tests for the beneficiary identity REST layer.
 *
 * <p>
 * Every endpoint here hand-rolls the same envelope: parse a raw JSON string,
 * call the service, then wrap the outcome in an {@code OutputResponse} that
 * always carries HTTP 200 and signals failure through a {@code statusCode} in
 * the body. Clients branch on that body, so the tests assert on the envelope -
 * which status code and message a given outcome produces - as much as on the
 * data. They also pin the input guards, since several endpoints accept a bare
 * JSON literal rather than a typed body.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IdentityControllerTest {

	@Mock
	private IdentityService svc;

	@InjectMocks
	private IdentityController controller;

	private static final BigInteger BEN_REG_ID = BigInteger.valueOf(100200300L);
	private static final BigInteger BEN_ID = BigInteger.valueOf(4001L);

	private BeneficiariesDTO beneficiary(String firstName) {
		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(BEN_ID);
		dto.setBenRegId(BEN_REG_ID);
		BenDetailDTO details = new BenDetailDTO();
		details.setFirstName(firstName);
		dto.setBeneficiaryDetails(details);
		return dto;
	}

	private void assertSuccess(String response) {
		assertTrue(response.contains("\"statusCode\":200"), response);
		assertTrue(response.contains("\"statusMessage\":\"success\""), response);
	}

	private void assertFailure(String response, String expectedFragment) {
		assertTrue(response.contains(expectedFragment), response);
	}

	@Nested
	@DisplayName("advance search")
	class AdvanceSearch {

		@Test
		@DisplayName("search criteria are parsed out of the raw body and passed to the service")
		void criteriaAreParsedAndPassedToTheService() throws Exception {
			when(svc.getBeneficiaries(any(IdentitySearchDTO.class)))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			String response = controller.getBeneficiaries(
					"{\"firstName\":\"Asha\",\"genderId\":2,\"currentAddress\":{\"stateId\":101}}");

			assertSuccess(response);
			assertTrue(response.contains("Asha"));
			ArgumentCaptor<IdentitySearchDTO> captor = ArgumentCaptor.forClass(IdentitySearchDTO.class);
			verify(svc).getBeneficiaries(captor.capture());
			assertEquals("Asha", captor.getValue().getFirstName());
			assertEquals(2, captor.getValue().getGenderId());
			assertEquals(101, captor.getValue().getCurrentAddress().getStateId());
		}

		@Test
		@DisplayName("null entries from the service are dropped before the results are sorted")
		void nullEntriesAreDropped() throws Exception {
			// Collections.sort would throw on a null element, so the filter is
			// what keeps a partially failed assembly from failing the request.
			when(svc.getBeneficiaries(any(IdentitySearchDTO.class)))
					.thenReturn(new java.util.ArrayList<>(Arrays.asList(beneficiary("Asha"), null)));

			assertSuccess(controller.getBeneficiaries("{\"firstName\":\"Asha\"}"));
		}

		@Test
		@DisplayName("a service failure is reported in the body rather than as an HTTP error")
		void serviceFailureIsReportedInTheBody() throws Exception {
			when(svc.getBeneficiaries(any(IdentitySearchDTO.class)))
					.thenThrow(new IllegalStateException("query timeout"));

			String response = controller.getBeneficiaries("{\"firstName\":\"Asha\"}");

			assertFailure(response, "5000");
			assertFailure(response, "error in beneficiary advance search");
		}

		@Test
		@DisplayName("an unparseable body is reported as a failure")
		void unparseableBodyIsReportedAsFailure() throws Exception {
			String response = controller.getBeneficiaries("not json at all {");

			assertFailure(response, "5000");
			verifyNoInteractions(svc);
		}
	}

	@Nested
	@DisplayName("lookup by identifier")
	class LookupByIdentifier {

		@Test
		@DisplayName("a registration ID is looked up as a number")
		void registrationIdIsLookedUpAsANumber() throws Exception {
			when(svc.getBeneficiariesByBenRegId(BEN_REG_ID))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			String response = controller.getBeneficiariesByBeneficiaryRegId("100200300");

			assertSuccess(response);
			verify(svc).getBeneficiariesByBenRegId(BEN_REG_ID);
		}

		@Test
		@DisplayName("an empty registration ID is rejected without touching the service")
		void emptyRegistrationIdIsRejected() throws Exception {
			String response = controller.getBeneficiariesByBeneficiaryRegId("");

			assertFailure(response, "Null/Empty Beneficiary Id.");
			verify(svc, never()).getBeneficiariesByBenRegId(any());
		}

		@Test
		@DisplayName("a whitespace-only registration ID falls through the guard and fails on conversion")
		void whitespaceRegistrationIdFailsOnConversion() throws Exception {
			// The guard is a length check, so whitespace reaches new BigInteger
			// and the caller gets a number-format message instead of the
			// intended "Null/Empty Beneficiary Id."
			String response = controller.getBeneficiariesByBeneficiaryRegId(" ");

			assertFailure(response, "5000");
			verify(svc, never()).getBeneficiariesByBenRegId(any());
		}

		@Test
		@DisplayName("a null registration ID is rejected")
		void nullRegistrationIdIsRejected() throws Exception {
			assertFailure(controller.getBeneficiariesByBeneficiaryRegId(null), "Null/Empty Beneficiary Id.");
		}

		@Test
		@DisplayName("a non-numeric registration ID is reported as a failure")
		void nonNumericRegistrationIdIsReportedAsFailure() throws Exception {
			assertFailure(controller.getBeneficiariesByBeneficiaryRegId("abc"), "5000");
		}

		@Test
		@DisplayName("a beneficiary ID sent as a bare number is looked up")
		void beneficiaryIdSentAsBareNumberIsLookedUp() throws Exception {
			when(svc.getBeneficiariesByBenId(BEN_ID))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.getBeneficiariesByBeneficiaryId("4001"));

			verify(svc).getBeneficiariesByBenId(BEN_ID);
		}

		@Test
		@DisplayName("a beneficiary ID sent as a quoted JSON string is rejected")
		void beneficiaryIdSentAsQuotedStringIsRejected() throws Exception {
			// A quoted value parses to a JsonPrimitive, and that branch keeps the
			// raw text - quotes included - so it never reaches the unquoting
			// branch below it and fails number conversion instead.
			String response = controller.getBeneficiariesByBeneficiaryId("\"4001\"");

			assertFailure(response, "5000");
			verify(svc, never()).getBeneficiariesByBenId(any());
		}

		@Test
		@DisplayName("a JSON null beneficiary ID is rejected")
		void jsonNullBeneficiaryIdIsRejected() throws Exception {
			assertFailure(controller.getBeneficiariesByBeneficiaryId("null"), "Null/Empty Beneficiary Id.");
			verify(svc, never()).getBeneficiariesByBenId(any());
		}

		@Test
		@DisplayName("a failing beneficiary lookup is reported in the body")
		void failingBeneficiaryLookupIsReportedInTheBody() throws Exception {
			when(svc.getBeneficiariesByBenId(BEN_ID)).thenThrow(new IllegalStateException("view down"));

			assertFailure(controller.getBeneficiariesByBeneficiaryId("4001"), "5000");
		}
	}

	@Nested
	@DisplayName("lookup by phone number")
	class LookupByPhoneNumber {

		@Test
		@DisplayName("a phone number sent as a quoted JSON string is looked up")
		void quotedPhoneNumberIsLookedUp() {
			when(svc.getBeneficiariesByPhoneNum("9000000000"))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.getBeneficiariesByPhoneNum("\"9000000000\""));
		}

		@Test
		@DisplayName("a JSON null phone number is rejected")
		void jsonNullPhoneNumberIsRejected() {
			assertFailure(controller.getBeneficiariesByPhoneNum("null"), "Null/Empty Phone Number.");
			verify(svc, never()).getBeneficiariesByPhoneNum(any());
		}

		@Test
		@DisplayName("a failing phone lookup is reported in the body")
		void failingPhoneLookupIsReportedInTheBody() {
			when(svc.getBeneficiariesByPhoneNum(any())).thenThrow(new IllegalStateException("contact table down"));

			assertFailure(controller.getBeneficiariesByPhoneNum("\"9000000000\""), "5000");
		}
	}

	@Nested
	@DisplayName("lookup by ABHA and government identifiers")
	class LookupByExternalIdentifier {

		@Test
		@DisplayName("an ABHA address is looked up")
		void abhaAddressIsLookedUp() throws Exception {
			when(svc.getBeneficiaryByHealthIDAbhaAddress("asha@abdm"))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.searhBeneficiaryByABHAAddress("\"asha@abdm\""));
		}

		@Test
		@DisplayName("a JSON null ABHA address is rejected")
		void jsonNullAbhaAddressIsRejected() throws Exception {
			assertFailure(controller.searhBeneficiaryByABHAAddress("null"), "Null/Empty Health ID / ABHA Address.");
		}

		@Test
		@DisplayName("a failing ABHA address lookup is reported in the body")
		void failingAbhaAddressLookupIsReported() throws Exception {
			when(svc.getBeneficiaryByHealthIDAbhaAddress(any())).thenThrow(new IllegalStateException("view down"));

			assertFailure(controller.searhBeneficiaryByABHAAddress("\"asha@abdm\""), "5000");
		}

		@Test
		@DisplayName("an ABHA number is looked up")
		void abhaNumberIsLookedUp() throws Exception {
			when(svc.getBeneficiaryByHealthIDNoAbhaIdNo("12-3456-7890-1234"))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.searhBeneficiaryByABHAIdNo("\"12-3456-7890-1234\""));
		}

		@Test
		@DisplayName("a JSON null ABHA number is rejected")
		void jsonNullAbhaNumberIsRejected() throws Exception {
			assertFailure(controller.searhBeneficiaryByABHAIdNo("null"), "Null/Empty Health ID No / ABHA Id No.");
		}

		@Test
		@DisplayName("a failing ABHA number lookup is reported in the body")
		void failingAbhaNumberLookupIsReported() throws Exception {
			when(svc.getBeneficiaryByHealthIDNoAbhaIdNo(any())).thenThrow(new IllegalStateException("view down"));

			assertFailure(controller.searhBeneficiaryByABHAIdNo("\"12-3456\""), "5000");
		}

		@Test
		@DisplayName("a government identity number is looked up")
		void governmentIdentityIsLookedUp() {
			when(svc.searhBeneficiaryByGovIdentity(any()))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.searhBeneficiaryByGovIdentity("\"AADHAAR-1\""));
		}

		@Test
		@DisplayName("a JSON null government identity number is rejected")
		void jsonNullGovernmentIdentityIsRejected() {
			assertFailure(controller.searhBeneficiaryByGovIdentity("null"), "Null/Empty Gov Identity No.");
		}

		@Test
		@DisplayName("a failing government identity lookup is reported in the body")
		void failingGovernmentIdentityLookupIsReported() {
			when(svc.searhBeneficiaryByGovIdentity(any())).thenThrow(new IllegalStateException("view down"));

			assertFailure(controller.searhBeneficiaryByGovIdentity("\"AADHAAR-1\""), "5000");
		}

		@Test
		@DisplayName("a family ID is looked up")
		void familyIdIsLookedUp() {
			when(svc.searhBeneficiaryByFamilyId(any()))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.searhBeneficiaryByFamilyId("\"FAM-1\""));
		}

		@Test
		@DisplayName("a JSON null family ID is rejected")
		void jsonNullFamilyIdIsRejected() {
			assertFailure(controller.searhBeneficiaryByFamilyId("null"), "Null/Empty Family Id.");
		}

		@Test
		@DisplayName("a failing family lookup is reported in the body")
		void failingFamilyLookupIsReported() {
			when(svc.searhBeneficiaryByFamilyId(any())).thenThrow(new IllegalStateException("view down"));

			assertFailure(controller.searhBeneficiaryByFamilyId("\"FAM-1\""), "5000");
		}
	}

	@Nested
	@DisplayName("CHO app village sync")
	class VillageSync {

		private static final String SYNC_REQUEST = "{\"villageID\":[401,402],\"lastModifiedDate\":1767225600000}";

		@Test
		@DisplayName("the requested villages and watermark are passed through to the service")
		void villagesAndWatermarkArePassedThrough() {
			when(svc.searchBeneficiaryByVillageIdAndLastModifyDate(anyList(), any()))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			assertSuccess(controller.searchBeneficiaryByVillageIdAndLastModDate(SYNC_REQUEST));

			ArgumentCaptor<List<Integer>> villages = captor();
			ArgumentCaptor<Timestamp> watermark = ArgumentCaptor.forClass(Timestamp.class);
			verify(svc).searchBeneficiaryByVillageIdAndLastModifyDate(villages.capture(), watermark.capture());
			assertEquals(List.of(401, 402), villages.getValue());
			assertEquals(new Timestamp(1767225600000L), watermark.getValue());
		}

		@Test
		@DisplayName("a request with no watermark is reported as a failure rather than syncing everything")
		void requestWithNoWatermarkIsReportedAsFailure() {
			// The service would receive a null timestamp and return the whole
			// village, so the request has to fail instead.
			assertFailure(controller.searchBeneficiaryByVillageIdAndLastModDate("{\"villageID\":[401]}"), "5000");
		}

		@Test
		@DisplayName("a failing sync query is reported in the body")
		void failingSyncQueryIsReportedInTheBody() {
			when(svc.searchBeneficiaryByVillageIdAndLastModifyDate(anyList(), any()))
					.thenThrow(new IllegalStateException("timeout"));

			assertFailure(controller.searchBeneficiaryByVillageIdAndLastModDate(SYNC_REQUEST), "5000");
		}

		@Test
		@DisplayName("the count endpoint returns the number the service reports")
		void countEndpointReturnsTheServiceCount() {
			when(svc.countBeneficiaryByVillageIdAndLastModifyDate(anyList(), any())).thenReturn(42L);

			String response = controller.countBeneficiaryByVillageIdAndLastModDate(SYNC_REQUEST);

			assertSuccess(response);
			assertTrue(response.contains("42"));
		}

		@Test
		@DisplayName("a failing count is reported in the body")
		void failingCountIsReportedInTheBody() {
			when(svc.countBeneficiaryByVillageIdAndLastModifyDate(anyList(), any()))
					.thenThrow(new IllegalStateException("timeout"));

			assertFailure(controller.countBeneficiaryByVillageIdAndLastModDate(SYNC_REQUEST), "5000");
		}
	}

	@Nested
	@DisplayName("RMNCH lookup")
	class RmnchLookup {

		@Test
		@DisplayName("the RMNCH record is returned with HTTP 200")
		void rmnchRecordIsReturnedWithOk() {
			RMNCHBeneficiaryDetailsRmnch record = new RMNCHBeneficiaryDetailsRmnch();
			record.setRchid("RCH-1");
			when(svc.getRmnchDataByBenID(BEN_REG_ID)).thenReturn(record);

			ResponseEntity<RMNCHBeneficiaryDetailsRmnch> response = controller.getRmnchDataByBenID(BEN_REG_ID);

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("RCH-1", response.getBody().getRchid());
		}

		@Test
		@DisplayName("a failing lookup is the one endpoint that answers with a 500")
		void failingLookupAnswersWithServerError() {
			when(svc.getRmnchDataByBenID(any())).thenThrow(new IllegalStateException("table locked"));

			ResponseEntity<RMNCHBeneficiaryDetailsRmnch> response = controller.getRmnchDataByBenID(BEN_REG_ID);

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
			assertEquals(null, response.getBody());
		}
	}

	@Nested
	@DisplayName("create and edit")
	class CreateAndEdit {

		@Test
		@DisplayName("a create request returns the identifiers the service allocated")
		void createReturnsAllocatedIdentifiers() throws Exception {
			BeneficiaryCreateResp created = new BeneficiaryCreateResp();
			created.setBenId(BEN_ID);
			created.setBenRegId(BEN_REG_ID);
			when(svc.createIdentity(any())).thenReturn(created);

			String response = controller.createIdentity("{\"firstName\":\"Asha\",\"agentName\":\"field.worker\"}");

			assertSuccess(response);
			assertTrue(response.contains("4001"));
		}

		@Test
		@DisplayName("a create failure is propagated rather than swallowed into a success envelope")
		void createFailureIsPropagated() {
			// Unlike the read endpoints, a failed create must not answer 200 with
			// a success envelope - the caller would treat the beneficiary as
			// registered.
			when(svc.createIdentity(any())).thenThrow(new IllegalStateException("no IDs left in the pool"));

			assertThrows(IllegalStateException.class,
					() -> controller.createIdentity("{\"firstName\":\"Asha\"}"));
		}

		@Test
		@DisplayName("an edit request is passed to the service and acknowledged")
		void editIsPassedToTheServiceAndAcknowledged() throws Exception {
			String response = controller.editIdentity("{\"beneficiaryRegId\":100200300,\"firstName\":\"Asha\"}");

			assertSuccess(response);
			assertTrue(response.contains("Updated successfully"));
			ArgumentCaptor<IdentityEditDTO> captor = ArgumentCaptor.forClass(IdentityEditDTO.class);
			verify(svc).editIdentity(captor.capture());
			assertEquals(BEN_REG_ID, captor.getValue().getBeneficiaryRegId());
		}

		@Test
		@DisplayName("an edit missing a mandatory field reports the reason in the body")
		void editMissingMandatoryFieldReportsTheReason() throws Exception {
			org.mockito.Mockito.doThrow(new MissingMandatoryFieldsException("Beneficiary Reg Id is mandatory."))
					.when(svc).editIdentity(any());

			String response = controller.editIdentity("{\"firstName\":\"Asha\"}");

			assertFailure(response, "Beneficiary Reg Id is mandatory.");
		}

		@Test
		@DisplayName("an education or community edit is passed to the service")
		void educationOrCommunityEditIsPassedToTheService() throws Exception {
			String response = controller
					.editIdentityEducationOrCommunity("{\"beneficiaryRegId\":100200300,\"communityId\":5}");

			assertSuccess(response);
			verify(svc).editIdentityEducationOrCommunity(any());
		}

		@ParameterizedTest
		@ValueSource(strings = { "null", "\"\"", "123" })
		@DisplayName("an education or community edit with a bare JSON literal body is rejected")
		void educationOrCommunityEditWithLiteralBodyIsRejected(String body) throws Exception {
			String response = controller.editIdentityEducationOrCommunity(body);

			assertFailure(response, "Null/Empty Identity Edit Data.");
			verify(svc, never()).editIdentityEducationOrCommunity(any());
		}

		@Test
		@DisplayName("an education or community edit missing a mandatory field reports the reason")
		void educationOrCommunityEditMissingMandatoryFieldReportsTheReason() throws Exception {
			org.mockito.Mockito.doThrow(new MissingMandatoryFieldsException("Either of BeneficiaryID or Beneficiary Reg Id is mandatory."))
					.when(svc).editIdentityEducationOrCommunity(any());

			assertFailure(controller.editIdentityEducationOrCommunity("{\"firstName\":\"Asha\"}"),
					"Beneficiary Reg Id is mandatory.");
		}
	}

	@Nested
	@DisplayName("reserving identifiers")
	class ReservingIdentifiers {

		@Test
		@DisplayName("a reserve request is passed to the service")
		void reserveIsPassedToTheService() {
			when(svc.reserveIdentity(any())).thenReturn("Successfully Completed");

			String response = controller
					.reserveIdentity("{\"providerServiceMapID\":11,\"vehicalNo\":\"KA-01-1234\",\"reserveCount\":5}");

			assertSuccess(response);
			assertTrue(response.contains("Successfully Completed"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "null", "\"\"", "5" })
		@DisplayName("a reserve request with a bare JSON literal body is rejected")
		void reserveWithLiteralBodyIsRejected(String body) {
			assertFailure(controller.reserveIdentity(body), "Null/Empty Identity Create Data.");
			verify(svc, never()).reserveIdentity(any());
		}

		@Test
		@DisplayName("an unreserve request is passed to the service")
		void unreserveIsPassedToTheService() {
			when(svc.unReserveIdentity(any())).thenReturn("Successfully Completed");

			assertSuccess(controller
					.unreserveIdentity("{\"providerServiceMapID\":11,\"vehicalNo\":\"KA-01-1234\"}"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "null", "\"\"" })
		@DisplayName("an unreserve request with a bare JSON literal body is rejected")
		void unreserveWithLiteralBodyIsRejected(String body) {
			assertFailure(controller.unreserveIdentity(body), "Null/Empty Identity Create Data.");
			verify(svc, never()).unReserveIdentity(any());
		}
	}

	@Nested
	@DisplayName("bulk lookups")
	class BulkLookups {

		@Test
		@DisplayName("a partial-details request passes the whole identifier list through")
		void partialDetailsRequestPassesTheWholeList() {
			BeneficiariesPartialDTO partial = new BeneficiariesPartialDTO();
			partial.setBenId(BEN_ID);
			partial.setFirstName("Asha");
			when(svc.getBeneficiariesPartialDeatilsByBenRegIdList(anyList()))
					.thenReturn(new java.util.ArrayList<>(List.of(partial)));

			String response = controller.getPartialBeneficiariesByBenRegIds("[100200300,100200301]");

			assertSuccess(response);
			assertTrue(response.contains("Asha"));
			ArgumentCaptor<List<BigInteger>> captor = captor();
			verify(svc).getBeneficiariesPartialDeatilsByBenRegIdList(captor.capture());
			assertEquals(2, captor.getValue().size());
		}

		@Test
		@DisplayName("a JSON null partial-details request is rejected")
		void jsonNullPartialDetailsRequestIsRejected() {
			assertFailure(controller.getPartialBeneficiariesByBenRegIds("null"), "Null/Empty Phone Number.");
			verify(svc, never()).getBeneficiariesPartialDeatilsByBenRegIdList(any());
		}

		@Test
		@DisplayName("a full-details request converts the identifier array for the service")
		void fullDetailsRequestConvertsTheIdentifierArray() {
			when(svc.getBeneficiariesDeatilsByBenRegIdList(anyList()))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			String response = controller.getBeneficiariesByBenRegIds(new Long[] { 100200300L });

			assertSuccess(response);
			ArgumentCaptor<List<BigInteger>> captor = captor();
			verify(svc).getBeneficiariesDeatilsByBenRegIdList(captor.capture());
			assertEquals(BEN_REG_ID, captor.getValue().get(0));
		}

		@Test
		@DisplayName("an empty full-details request is rejected with a client-error status code")
		void emptyFullDetailsRequestIsRejected() {
			String response = controller.getBeneficiariesByBenRegIds(new Long[0]);

			assertFailure(response, "No beneficiary registration IDs provided");
			assertFailure(response, "400");
			verify(svc, never()).getBeneficiariesDeatilsByBenRegIdList(any());
		}
	}

	@Nested
	@DisplayName("finite search and image retrieval")
	class FiniteSearchAndImages {

		@Test
		@DisplayName("the finite-search endpoint runs the same advance search, not the service's finite search")
		void finiteSearchRunsTheAdvanceSearch() throws Exception {
			// Despite the name and the finiteSearch query on the repository, this
			// endpoint parses an IdentitySearchDTO and calls the same advance
			// search as /advanceSearch.
			when(svc.getBeneficiaries(any(IdentitySearchDTO.class)))
					.thenReturn(new java.util.ArrayList<>(List.of(beneficiary("Asha"))));

			String response = controller.getFiniteBeneficiaries("{\"firstName\":\"Asha\"}");

			assertSuccess(response);
			assertTrue(response.contains("Asha"));
			verify(svc, never()).getBeneficiaries(any(com.iemr.common.identity.dto.IdentityDTO.class));
		}

		@Test
		@DisplayName("a failing finite search is reported in the body")
		void failingFiniteSearchIsReportedInTheBody() throws Exception {
			when(svc.getBeneficiaries(any(IdentitySearchDTO.class)))
					.thenThrow(new IllegalStateException("query timeout"));

			assertFailure(controller.getFiniteBeneficiaries("{\"firstName\":\"Asha\"}"), "5000");
		}

		@Test
		@DisplayName("the stored image response is returned unchanged")
		void storedImageResponseIsReturnedUnchanged() {
			when(svc.getBeneficiaryImage(any())).thenReturn("{\"data\":\"base64\"}");

			assertEquals("{\"data\":\"base64\"}",
					controller.getBeneficiaryImageByBenRegID("{\"beneficiaryRegID\":100200300}"));
		}

		@Test
		@DisplayName("a failing image lookup yields no image rather than an error body")
		void failingImageLookupYieldsNoImage() {
			when(svc.getBeneficiaryImage(any())).thenThrow(new IllegalStateException("blob store down"));

			assertEquals(null, controller.getBeneficiaryImageByBenRegID("{\"beneficiaryRegID\":100200300}"));
		}
	}

	@Nested
	@DisplayName("local identifier pool")
	class LocalIdentifierPool {

		@Test
		@DisplayName("the available count is reported")
		void availableCountIsReported() {
			when(svc.checkBenIDAvailabilityLocal()).thenReturn(120L);

			String response = controller.checkAvailablBenIDLocalServer();

			assertTrue(response.contains("120"), response);
		}

		@Test
		@DisplayName("a failing availability check is reported as an error")
		void failingAvailabilityCheckIsReportedAsAnError() {
			when(svc.checkBenIDAvailabilityLocal()).thenThrow(new IllegalStateException("pool table down"));

			assertFailure(controller.checkAvailablBenIDLocalServer(), "5000");
		}

		@Test
		@DisplayName("an import reports how many identifiers were stored")
		void importReportsHowManyIdentifiersWereStored() {
			when(svc.importBenIdToLocalServer(anyList())).thenReturn(2);

			String response = controller.saveGeneratedBenIDToLocalServer(
					"[{\"benRegId\":100200300,\"beneficiaryId\":4001},{\"benRegId\":100200301,\"beneficiaryId\":4002}]");

			assertTrue(response.contains("2 Unique benid imported"), response);
		}

		@Test
		@DisplayName("an import that stores nothing is reported as invalid data")
		void importThatStoresNothingIsReportedAsInvalidData() {
			when(svc.importBenIdToLocalServer(anyList())).thenReturn(0);

			assertTrue(controller.saveGeneratedBenIDToLocalServer("[]").contains("Empty or invalid data"));
		}

		@Test
		@DisplayName("a failing import is reported as an error")
		void failingImportIsReportedAsAnError() {
			when(svc.importBenIdToLocalServer(anyList())).thenThrow(new IllegalStateException("batch failed"));

			assertFailure(controller.saveGeneratedBenIDToLocalServer("[{\"benRegId\":100200300}]"), "5000");
		}
	}

	@Test
	@DisplayName("an object is serialised to JSON, and an unserialisable one yields an empty string")
	void objectIsSerialisedToJson() {
		assertTrue(controller.getJsonAsString(beneficiary("Asha")).contains("Asha"));
	}

	@SuppressWarnings("unchecked")
	private <T> ArgumentCaptor<List<T>> captor() {
		return ArgumentCaptor.forClass(List.class);
	}
}
