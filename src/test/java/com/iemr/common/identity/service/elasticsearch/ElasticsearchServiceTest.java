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
package com.iemr.common.identity.service.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.util.ObjectBuilder;

import com.iemr.common.identity.dto.BeneficiariesESDTO;
import com.iemr.common.identity.repo.BenAddressRepo;
import com.iemr.common.identity.repo.BenDetailRepo;
import com.iemr.common.identity.repo.V_BenAdvanceSearchRepo;

/**
 * Tests for the Elasticsearch-backed beneficiary search.
 *
 * <p>
 * Nearly all of this service is a query built out of nested lambdas and a
 * projection of the hit back into the wide map shape the 1097 call-centre UI
 * expects. Two things can go wrong there and neither shows up at compile time:
 * the query DSL can be assembled into something the client rejects (a boost on
 * a clause that cannot carry one, a term on a field that is not a keyword), and
 * the projection can quietly drop or rename a field the UI reads.
 *
 * <p>
 * The client mock therefore <em>applies</em> the builder lambda it is handed
 * against a real {@link SearchRequest.Builder} instead of ignoring it, so an
 * unbuildable query fails the test. The service swallows search failures and
 * falls back to the database, so tests assert the fallback happened rather than
 * expecting an exception.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ElasticsearchServiceTest {

	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private BenDetailRepo benDetailRepo;
	@Mock
	private BenAddressRepo benAddressRepo;
	@Mock
	private V_BenAdvanceSearchRepo advanceSearchRepo;

	@InjectMocks
	private ElasticsearchService service;

	/** The request the client mock actually built, for assertions on the DSL. */
	private SearchRequest builtRequest;

	@BeforeEach
	void configureService() {
		ReflectionTestUtils.setField(service, "beneficiaryIndex", "beneficiary");
		ReflectionTestUtils.setField(service, "esEnabled", true);
		ReflectionTestUtils.setField(service, "v_BenAdvanceSearchRepo", advanceSearchRepo);
		builtRequest = null;
	}

	/**
	 * Stubs the client so the builder lambda is applied for real and the given
	 * documents come back as hits.
	 */
	@SuppressWarnings("unchecked")
	private void stubSearchReturning(BeneficiariesESDTO... documents) throws IOException {
		when(esClient.search(any(Function.class), any(Class.class))).thenAnswer(invocation -> {
			Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> builder = invocation.getArgument(0);
			builtRequest = builder.apply(new SearchRequest.Builder()).build();
			return searchResponse(documents);
		});
	}

	/** Stubs the client so the query is still built, but the search then fails. */
	@SuppressWarnings("unchecked")
	private void stubSearchFailing() throws IOException {
		when(esClient.search(any(Function.class), any(Class.class))).thenAnswer(invocation -> {
			Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> builder = invocation.getArgument(0);
			builtRequest = builder.apply(new SearchRequest.Builder()).build();
			throw new IOException("index unavailable");
		});
	}

	private SearchResponse<BeneficiariesESDTO> searchResponse(BeneficiariesESDTO... documents) {
		List<Hit<BeneficiariesESDTO>> hits = new ArrayList<>();
		for (int i = 0; i < documents.length; i++) {
			final BeneficiariesESDTO document = documents[i];
			final String id = String.valueOf(i);
			hits.add(Hit.of(h -> h.index("beneficiary").id(id).score(9.5).source(document)));
		}
		return SearchResponse.of(r -> r.took(5).timedOut(false)
				.shards(s -> s.total(1).successful(1).failed(0))
				.hits(h -> h.total(t -> t.value(hits.size()).relation(TotalHitsRelation.Eq)).hits(hits)));
	}

	private BeneficiariesESDTO document() {
		BeneficiariesESDTO dto = new BeneficiariesESDTO();
		dto.setBenRegId(100200300L);
		dto.setBeneficiaryID("4001");
		dto.setFirstName("Asha");
		dto.setLastName("Devi");
		dto.setGenderID(2);
		dto.setGenderName("Female");
		dto.setAge(30);
		dto.setPhoneNum("9000000000");
		dto.setStateID(101);
		dto.setStateName("Karnataka");
		dto.setDistrictID(201);
		dto.setDistrictName("Bengaluru");
		dto.setBlockID(301);
		dto.setBlockName("North");
		dto.setVillageID(401);
		dto.setVillageName("Yelahanka");
		dto.setPinCode("560064");
		dto.setFamilyID("FAM-1");
		return dto;
	}

	/** The 31-column projection the database fallback query returns. */
	private Object[] databaseRow() {
		Object[] row = new Object[31];
		row[0] = 100200300L; // beneficiaryRegID
		row[1] = "4001"; // beneficiaryID
		row[2] = "Asha";
		row[3] = "Rani";
		row[4] = "Devi";
		row[5] = 2; // genderID
		row[6] = "Female";
		row[7] = Timestamp.valueOf("1996-01-01 00:00:00"); // dob
		row[8] = 30; // age
		row[9] = "Ram"; // fatherName
		row[10] = "Suresh"; // spouseName
		row[11] = "1"; // maritalStatusID
		row[12] = "Married";
		row[13] = "no"; // isHIVPos
		row[14] = "field.worker";
		row[15] = Timestamp.valueOf("2026-01-01 00:00:00");
		row[16] = 1767225600000L; // lastModDate
		row[17] = 77L; // benAccountID
		row[18] = 101; // stateID
		row[19] = "Karnataka";
		row[20] = 201;
		row[21] = "Bengaluru";
		row[22] = 301;
		row[23] = "North";
		row[24] = "560064";
		row[25] = 501; // servicePointID
		row[26] = "PHC Yelahanka";
		row[27] = 3; // parkingPlaceID
		row[28] = "9000000000";
		row[29] = 401; // villageID
		row[30] = "Yelahanka";
		return row;
	}

	@Nested
	@DisplayName("universal search query construction")
	class UniversalSearchQueryConstruction {

		@Test
		@DisplayName("a single-word name query builds against the configured index")
		void singleWordQueryBuildsAgainstConfiguredIndex() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("Asha", null);

			assertNotNull(builtRequest);
			assertEquals(Collections.singletonList("beneficiary"), builtRequest.index());
			assertEquals(100, builtRequest.size());
		}

		/** healthID, abhaID, beneficiaryID, benId and aadharNo, on every query. */
		private static final int IDENTIFIER_CLAUSES = 5;

		@Test
		@DisplayName("a multi-word query builds one clause group per word alongside the identifier clauses")
		void multiWordQueryBuildsAClauseGroupPerWord() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("Asha Rani Devi", null);

			assertNotNull(builtRequest);
			assertEquals(3 + IDENTIFIER_CLAUSES,
					builtRequest.query().functionScore().query().bool().should().size());
		}

		@Test
		@DisplayName("a numeric query adds the phone and identifier prefix clauses a name query does not")
		void numericQueryAddsPrefixClauses() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("100200300", null);

			// Five identifier terms, four identifier prefixes, one phone
			// wildcard, and the benRegId and benAccountID numeric terms.
			assertEquals(12, builtRequest.query().functionScore().query().bool().should().size());
		}

		@Test
		@DisplayName("a numeric query matching the user's own village and block boosts those clauses")
		void numericQueryMatchingUsersLocationBoostsThoseClauses() throws Exception {
			stubSearchReturning(document());
			when(benAddressRepo.getUserLocation(42))
					.thenReturn(Collections.singletonList(new Object[] { 11, 401, 401, 501 }));

			service.universalSearch("401", 42);

			// The village and block clauses are added on top of the five
			// identifier terms, four prefixes and two numeric ID terms; the
			// phone wildcard needs four digits, so it is absent.
			assertEquals(13, builtRequest.query().functionScore().query().bool().should().size());
		}

		@Test
		@DisplayName("a short numeric query omits the phone-contains wildcard")
		void shortNumericQueryOmitsPhoneWildcard() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("401", null);

			assertEquals(11, builtRequest.query().functionScore().query().bool().should().size());
		}

		@ParameterizedTest
		@ValueSource(strings = { "A", "As", "Asha", "Asha Rani", "  Asha  " })
		@DisplayName("queries of any length build without tripping the fuzzy and wildcard thresholds")
		void queriesOfAnyLengthBuild(String query) throws Exception {
			stubSearchReturning(document());

			assertEquals(1, service.universalSearch(query, null).size());
			assertNotNull(builtRequest);
		}

		@Test
		@DisplayName("a numeric query is given the lower relevance floor an ID lookup needs")
		void numericQueryUsesLowerRelevanceFloor() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("100200300", null);

			assertEquals(1.0d, builtRequest.minScore());
		}

		@Test
		@DisplayName("a name query is given a higher relevance floor to keep noise out")
		void nameQueryUsesHigherRelevanceFloor() throws Exception {
			stubSearchReturning(document());

			service.universalSearch("Asha", null);

			assertEquals(1.5d, builtRequest.minScore());
		}

		@Test
		@DisplayName("results are ranked toward the searching user's own village and block")
		void resultsAreRankedTowardTheUsersLocation() throws Exception {
			stubSearchReturning(document());
			when(benAddressRepo.getUserLocation(42))
					.thenReturn(Collections.singletonList(new Object[] { 11, 301, 401, 501 }));

			service.universalSearch("Asha", 42);

			assertEquals(2, builtRequest.query().functionScore().functions().size());
		}

		@Test
		@DisplayName("an unknown user contributes no location ranking")
		void unknownUserContributesNoLocationRanking() throws Exception {
			stubSearchReturning(document());
			when(benAddressRepo.getUserLocation(42)).thenReturn(Collections.emptyList());

			service.universalSearch("Asha", 42);

			assertTrue(builtRequest.query().functionScore().functions().isEmpty());
		}

		@Test
		@DisplayName("a failing user-location lookup does not stop the search")
		void failingUserLocationLookupDoesNotStopTheSearch() throws Exception {
			stubSearchReturning(document());
			when(benAddressRepo.getUserLocation(42)).thenThrow(new IllegalStateException("view unavailable"));

			assertEquals(1, service.universalSearch("Asha", 42).size());
		}

		@Test
		@DisplayName("the single-argument overload searches without location ranking")
		void singleArgumentOverloadSearchesWithoutLocationRanking() throws Exception {
			stubSearchReturning(document());

			assertEquals(1, service.universalSearch("Asha").size());

			verify(benAddressRepo, never()).getUserLocation(any());
		}
	}

	@Nested
	@DisplayName("universal search result projection")
	class UniversalSearchResultProjection {

		@Test
		@DisplayName("a hit is projected into the wide shape the call-centre UI reads")
		void hitIsProjectedIntoTheWideShape() throws Exception {
			stubSearchReturning(document());

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			assertEquals(100200300L, result.get("beneficiaryRegID"));
			assertEquals("Asha", result.get("firstName"));
			assertEquals("Years", result.get("ageUnits"));
			assertEquals(30, result.get("age"));
			assertEquals(30, result.get("actualAge"));
			assertEquals("FAM-1", result.get("familyID"));
			assertEquals("FAM-1", result.get("familyId"));
			assertEquals(9.5, result.get("_score"));
		}

		@Test
		@DisplayName("absent optional names are projected as blanks rather than nulls")
		void absentOptionalNamesAreProjectedAsBlanks() throws Exception {
			BeneficiariesESDTO document = document();
			document.setMiddleName(null);
			document.setLastName(null);
			document.setFatherName(null);
			document.setSpouseName(null);
			document.setMaritalStatusID(null);
			document.setMaritalStatusName(null);
			document.setIsHIVPos(null);
			stubSearchReturning(document);

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			assertEquals("", result.get("middleName"));
			assertEquals("", result.get("lastName"));
			assertEquals("", result.get("fatherName"));
			assertEquals("", result.get("spouseName"));
			assertEquals("", result.get("maritalStatusID"));
			assertEquals("", result.get("isHIVPos"));
		}

		@Test
		@DisplayName("the address hierarchy is projected as the nested objects the UI expects")
		void addressHierarchyIsProjectedAsNestedObjects() throws Exception {
			stubSearchReturning(document());

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			Map<String, Object> demographics = (Map<String, Object>) result.get("i_bendemographics");
			assertEquals(101, demographics.get("stateID"));
			assertEquals(401, demographics.get("villageID"));
			assertEquals(401, demographics.get("districtBranchID"));
			@SuppressWarnings("unchecked")
			Map<String, Object> state = (Map<String, Object>) demographics.get("m_state");
			assertEquals("Karnataka", state.get("stateName"));
			assertEquals(1, state.get("countryID"));
			@SuppressWarnings("unchecked")
			Map<String, Object> branch = (Map<String, Object>) demographics.get("m_districtbranchmapping");
			assertEquals("560064", branch.get("pinCode"));
		}

		@Test
		@DisplayName("a phone number on the document becomes a self-relationship phone map")
		void phoneNumberBecomesSelfRelationshipPhoneMap() throws Exception {
			stubSearchReturning(document());

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> phoneMaps = (List<Map<String, Object>>) result.get("benPhoneMaps");
			assertEquals(1, phoneMaps.size());
			assertEquals("9000000000", phoneMaps.get(0).get("phoneNo"));
			@SuppressWarnings("unchecked")
			Map<String, Object> relation = (Map<String, Object>) phoneMaps.get(0).get("benRelationshipType");
			assertEquals("Self", relation.get("benRelationshipType"));
		}

		@Test
		@DisplayName("a document with no phone number yields no phone maps")
		void documentWithNoPhoneNumberYieldsNoPhoneMaps() throws Exception {
			BeneficiariesESDTO document = document();
			document.setPhoneNum("");
			stubSearchReturning(document);

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> phoneMaps = (List<Map<String, Object>>) result.get("benPhoneMaps");
			assertTrue(phoneMaps.isEmpty());
		}

		@Test
		@DisplayName("an ABHA number on the document is projected without a database round trip")
		void abhaOnDocumentAvoidsDatabaseRoundTrip() throws Exception {
			BeneficiariesESDTO document = document();
			document.setAbhaID("12-3456-7890-1234");
			document.setHealthID("asha@abdm");
			document.setAbhaCreatedDate("2026-01-15");
			stubSearchReturning(document);

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> abhaDetails = (List<Map<String, Object>>) result.get("abhaDetails");
			assertEquals(1, abhaDetails.size());
			assertEquals("12-3456-7890-1234", abhaDetails.get(0).get("healthIDNumber"));
			assertNotNull(abhaDetails.get(0).get("createdDate"));
			verify(advanceSearchRepo, never()).getBenAbhaDetailsByBenRegID(any());
		}

		@ParameterizedTest
		@ValueSource(strings = { "2026-01-15", "2026-01-15 10:30:00", "2026-01-15 10:30:00.5" })
		@DisplayName("every ABHA date format the index stores is parsed to epoch millis")
		void everyAbhaDateFormatIsParsed(String storedDate) throws Exception {
			BeneficiariesESDTO document = document();
			document.setAbhaID("12-3456-7890-1234");
			document.setAbhaCreatedDate(storedDate);
			stubSearchReturning(document);

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> abhaDetails = (List<Map<String, Object>>) result.get("abhaDetails");
			assertNotNull(abhaDetails.get(0).get("createdDate"));
		}

		@Test
		@DisplayName("an unparseable ABHA date drops the whole projection rather than half of it")
		void unparseableAbhaDateDropsTheProjection() throws Exception {
			// The mapper aborts and returns null, and the caller filters it out -
			// a beneficiary is dropped from the results rather than returned with
			// a corrupt ABHA block.
			BeneficiariesESDTO document = document();
			document.setAbhaID("12-3456-7890-1234");
			document.setAbhaCreatedDate("15/01/2026");
			stubSearchReturning(document);

			assertTrue(service.universalSearch("Asha", null).isEmpty());
		}

		@Test
		@DisplayName("a document without ABHA falls back to the ABHA view")
		void documentWithoutAbhaFallsBackToTheView() throws Exception {
			stubSearchReturning(document());
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegID(any())).thenReturn(Collections
					.singletonList(new Object[] { "asha@abdm", Timestamp.valueOf("2026-01-15 10:30:00") }));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> abhaDetails = (List<Map<String, Object>>) result.get("abhaDetails");
			assertEquals(1, abhaDetails.size());
			assertEquals("asha@abdm", abhaDetails.get(0).get("healthID"));
			assertEquals(Timestamp.valueOf("2026-01-15 10:30:00").getTime(), abhaDetails.get(0).get("createdDate"));
		}

		@Test
		@DisplayName("a failing ABHA view leaves the beneficiary without ABHA details rather than dropping it")
		void failingAbhaViewLeavesBeneficiaryWithoutAbhaDetails() throws Exception {
			stubSearchReturning(document());
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegID(any()))
					.thenThrow(new IllegalStateException("view unavailable"));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> abhaDetails = (List<Map<String, Object>>) result.get("abhaDetails");
			assertTrue(abhaDetails.isEmpty());
		}
	}

	@Nested
	@DisplayName("database fallback")
	class DatabaseFallback {

		@Test
		@DisplayName("an index with no hits falls back to the database")
		void noHitsFallsBackToTheDatabase() throws Exception {
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(databaseRow()));

			List<Map<String, Object>> results = service.universalSearch("Asha", null);

			assertEquals(1, results.size());
			assertEquals("Asha", results.get(0).get("firstName"));
		}

		@Test
		@DisplayName("an unreachable index falls back to the database")
		void unreachableIndexFallsBackToTheDatabase() throws Exception {
			stubSearchFailing();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(databaseRow()));

			assertEquals(1, service.universalSearch("Asha", null).size());
		}

		@Test
		@DisplayName("a failing fallback yields no results rather than an error")
		void failingFallbackYieldsNoResults() throws Exception {
			stubSearchFailing();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenThrow(new IllegalStateException("db down"));

			assertTrue(service.universalSearch("Asha", null).isEmpty());
		}

		@Test
		@DisplayName("a database row is projected into the same shape as an index hit")
		void databaseRowIsProjectedIntoTheSameShape() throws Exception {
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(databaseRow()));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			assertEquals(100200300L, result.get("beneficiaryRegID"));
			assertEquals("4001", result.get("beneficiaryID"));
			assertEquals("Rani", result.get("middleName"));
			assertEquals("Married", result.get("maritalStatusName"));
			assertEquals("Years", result.get("ageUnits"));
			assertNotNull(result.get("dOB"));
			@SuppressWarnings("unchecked")
			Map<String, Object> demographics = (Map<String, Object>) result.get("i_bendemographics");
			assertEquals("PHC Yelahanka", demographics.get("servicePointName"));
			assertEquals(3, demographics.get("parkingPlaceID"));
		}

		@Test
		@DisplayName("phone numbers for a fallback row are fetched and numbered in order")
		void phoneNumbersForFallbackRowAreNumberedInOrder() throws Exception {
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(databaseRow()));
			when(benDetailRepo.findPhoneNumbersByBeneficiaryId(100200300L)).thenReturn(
					Arrays.asList(new Object[] { "9000000001", "Mobile" }, new Object[] { "9000000002", null },
							new Object[] { "", "Mobile" }, new Object[] { null, "Mobile" }));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> phoneMaps = (List<Map<String, Object>>) result.get("benPhoneMaps");
			assertEquals(2, phoneMaps.size());
			assertEquals(1L, phoneMaps.get(0).get("benPhMapID"));
			assertEquals(2L, phoneMaps.get(1).get("benPhMapID"));
			@SuppressWarnings("unchecked")
			Map<String, Object> relation = (Map<String, Object>) phoneMaps.get(1).get("benRelationshipType");
			assertEquals("Self", relation.get("benRelationshipType"));
		}

		@Test
		@DisplayName("a failing phone lookup leaves the row without phone maps")
		void failingPhoneLookupLeavesRowWithoutPhoneMaps() throws Exception {
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(databaseRow()));
			when(benDetailRepo.findPhoneNumbersByBeneficiaryId(anyLong()))
					.thenThrow(new IllegalStateException("db down"));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> phoneMaps = (List<Map<String, Object>>) result.get("benPhoneMaps");
			assertTrue(phoneMaps.isEmpty());
		}

		@Test
		@DisplayName("the numeric column types the driver may hand back are all coerced")
		void everyNumericColumnTypeIsCoerced() throws Exception {
			Object[] row = databaseRow();
			row[0] = BigDecimal.valueOf(100200300L); // regId as BigDecimal
			row[5] = "2"; // genderID as text
			row[8] = 30L; // age as Long
			row[16] = "1767225600000"; // lastModDate as text
			row[17] = BigDecimal.valueOf(77L);
			row[7] = new java.sql.Date(System.currentTimeMillis());
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(row));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			assertEquals(100200300L, result.get("beneficiaryRegID"));
			assertEquals(2, result.get("genderID"));
			assertEquals(30, result.get("age"));
			assertEquals(1767225600000L, result.get("lastModDate"));
			assertNotNull(result.get("dOB"));
		}

		@Test
		@DisplayName("an unparseable numeric column becomes null rather than failing the row")
		void unparseableNumericColumnBecomesNull() throws Exception {
			Object[] row = databaseRow();
			row[5] = "not-a-number";
			row[16] = "not-a-number";
			row[7] = "not-a-date";
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(row));

			Map<String, Object> result = service.universalSearch("Asha", null).get(0);

			assertNull(result.get("genderID"));
			assertNull(result.get("lastModDate"));
			assertNull(result.get("dOB"));
		}

		@Test
		@DisplayName("a row that is too short is reported as a partial result rather than failing the search")
		void shortRowIsReportedAsPartialResult() throws Exception {
			stubSearchReturning();
			when(benDetailRepo.searchBeneficiaries("Asha")).thenReturn(Collections.singletonList(new Object[] { 1L }));

			List<Map<String, Object>> results = service.universalSearch("Asha", null);

			assertEquals(1, results.size());
			assertTrue(results.get(0).isEmpty());
		}
	}

	@Nested
	@DisplayName("advanced search")
	class AdvancedSearch {

		@Test
		@DisplayName("exact-match criteria are placed in filter context so the index can cache them")
		void exactMatchCriteriaGoInFilterContext() throws Exception {
			stubSearchReturning(document());

			service.advancedSearch(null, null, null, 2, null, 101, 201, 301, 401, null, null, null, null, "4001",
					null, null, null);

			assertNotNull(builtRequest);
			// genderID, stateID, districtID, blockID, villageID and beneficiaryID
			assertEquals(6, builtRequest.query().bool().filter().size());
			assertTrue(builtRequest.query().bool().must().isEmpty());
		}

		@Test
		@DisplayName("name criteria are placed in must context so they contribute to the score")
		void nameCriteriaGoInMustContext() throws Exception {
			stubSearchReturning(document());

			service.advancedSearch("Asha", "Rani", "Devi", null, null, null, null, null, null, null, null, null,
					null, null, null, null, null);

			assertEquals(3, builtRequest.query().bool().must().size());
			assertTrue(builtRequest.query().bool().filter().isEmpty());
		}

		@ParameterizedTest
		@ValueSource(strings = { "", "   " })
		@DisplayName("a blank name is not treated as a criterion")
		void blankNameIsNotACriterion(String blank) throws Exception {
			stubSearchReturning(document());

			service.advancedSearch(blank, blank, blank, null, null, null, null, null, null, null, null, null, null,
					blank, null, null, null);

			assertTrue(builtRequest.query().bool().must().isEmpty());
			assertTrue(builtRequest.query().bool().filter().isEmpty());
		}

		@Test
		@DisplayName("every optional criterion together still builds one valid query")
		void everyOptionalCriterionTogetherStillBuilds() throws Exception {
			stubSearchReturning(document());

			List<Map<String, Object>> results = service.advancedSearch("Asha", "Rani", "Devi", 2, new Date(), 101,
					201, 301, 401, "Ram", "Suresh", "Married", "9000000000", "4001", "asha@abdm", "123456789012",
					42);

			assertEquals(1, results.size());
			assertNotNull(builtRequest);
		}

		@Test
		@DisplayName("a search with no criteria at all still builds a valid match-everything query")
		void searchWithNoCriteriaStillBuilds() throws Exception {
			stubSearchReturning(document());

			assertEquals(1, service.advancedSearch(null, null, null, null, null, null, null, null, null, null, null,
					null, null, null, null, null, null).size());
		}

		@Test
		@DisplayName("an unreachable index falls back to the database advanced search")
		void unreachableIndexFallsBackToDatabaseAdvancedSearch() throws Exception {
			stubSearchFailing();
			when(benDetailRepo.advancedSearchBeneficiaries(any(), any(), any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any(), any())).thenReturn(Collections.singletonList(databaseRow()));

			List<Map<String, Object>> results = service.advancedSearch("Asha", null, "Devi", 2, null, 101, null,
					null, null, null, null, null, null, null, null, null, null);

			assertEquals(1, results.size());
			assertEquals("Asha", results.get(0).get("firstName"));
		}

		@Test
		@DisplayName("a failing database advanced search yields no results")
		void failingDatabaseAdvancedSearchYieldsNoResults() throws Exception {
			stubSearchFailing();
			when(benDetailRepo.advancedSearchBeneficiaries(any(), any(), any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any(), any())).thenThrow(new IllegalStateException("db down"));

			assertTrue(service.advancedSearch("Asha", null, null, null, null, null, null, null, null, null, null,
					null, null, null, null, null, null).isEmpty());
		}

		@Test
		@DisplayName("results are ranked toward the searching user's location when a user is supplied")
		void resultsAreRankedTowardTheUsersLocation() throws Exception {
			stubSearchReturning(document());
			when(benAddressRepo.getUserLocation(42))
					.thenReturn(Collections.singletonList(new Object[] { 11, 301, 401, 501 }));

			service.advancedSearch("Asha", null, null, null, null, null, null, null, null, null, null, null, null,
					null, null, null, 42);

			verify(benAddressRepo).getUserLocation(42);
		}
	}
}
