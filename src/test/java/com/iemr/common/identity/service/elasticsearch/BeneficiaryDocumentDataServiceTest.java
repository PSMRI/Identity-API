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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.V_BenAdvanceSearchRepo;

/**
 * Tests for the batch loader that builds Elasticsearch documents.
 *
 * <p>
 * This is the query that makes a full re-index feasible: one 40-column
 * projection for a page of beneficiaries, plus one ABHA query for the same page,
 * joined in memory. Two things make it fragile. The projection is read
 * positionally, so a column added or reordered in the repository query silently
 * shifts every field after it; and the ABHA view is optional - it may not exist
 * on every deployment - so a failure there must degrade to documents without
 * ABHA rather than losing the page.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BeneficiaryDocumentDataServiceTest {

	@Mock
	private BenMappingRepo mappingRepo;
	@Mock
	private V_BenAdvanceSearchRepo advanceSearchRepo;

	@InjectMocks
	private BeneficiaryDocumentDataService service;

	private static final BigInteger BEN_REG_ID = BigInteger.valueOf(100200300L);

	/** The 40-column projection, in the order the repository query selects. */
	private Object[] projectionRow() {
		return new Object[] { 100200300L, "4001", "Asha", "Rani", "Devi", 2, "Female",
				Timestamp.valueOf("1996-01-15 00:00:00"), 30, "Ram", "Suresh", 1, "Married", "no", "field.worker",
				Timestamp.valueOf("2026-01-01 00:00:00"), 1767225600000L, 77L, "9000000000", "FAM-1", 101,
				"Karnataka", 201, "Bengaluru", 301, "North", 401, "Yelahanka", "560064", 501, "PHC Yelahanka", 3,
				102, "Kerala", 202, "Kochi", 302, "South", 402, "Kakkanad" };
	}

	private void useReflectionForAdvanceSearchRepo() {
		ReflectionTestUtils.setField(service, "v_BenAdvanceSearchRepo", advanceSearchRepo);
	}

	@Nested
	@DisplayName("batch loading")
	class BatchLoading {

		@Test
		@DisplayName("an empty or absent identifier list is not sent to the database")
		void emptyIdentifierListIsNotQueried() {
			assertTrue(service.getBeneficiariesBatch(Collections.emptyList()).isEmpty());
			assertTrue(service.getBeneficiariesBatch(null).isEmpty());

			verify(mappingRepo, never()).findCompleteDataByBenRegIds(anyList());
		}

		@Test
		@DisplayName("the projection is read into the document field by field")
		void projectionIsReadFieldByField() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));

			BeneficiaryDocument document = service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0);

			assertEquals(100200300L, document.getBenRegId());
			assertEquals("4001", document.getBenId());
			assertEquals("4001", document.getBeneficiaryID());
			assertEquals("Asha", document.getFirstName());
			assertEquals("Rani", document.getMiddleName());
			assertEquals("Devi", document.getLastName());
			assertEquals(2, document.getGenderID());
			assertEquals("Female", document.getGenderName());
			assertEquals("Female", document.getGender());
			assertEquals(30, document.getAge());
			assertEquals("Ram", document.getFatherName());
			assertEquals("Suresh", document.getSpouseName());
			assertEquals(1, document.getMaritalStatusID());
			assertEquals("Married", document.getMaritalStatusName());
			assertEquals("no", document.getIsHIVPos());
			assertEquals("field.worker", document.getCreatedBy());
			assertEquals(1767225600000L, document.getLastModDate());
			assertEquals(77L, document.getBenAccountID());
			assertEquals("9000000000", document.getPhoneNum());
			assertEquals("FAM-1", document.getFamilyID());
			assertNotNull(document.getDOB());
			assertNotNull(document.getCreatedDate());
		}

		@Test
		@DisplayName("the current and permanent address columns land in their own fields")
		void currentAndPermanentAddressesLandSeparately() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));

			BeneficiaryDocument document = service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0);

			assertEquals(101, document.getStateID());
			assertEquals("Karnataka", document.getStateName());
			assertEquals(401, document.getVillageID());
			assertEquals("560064", document.getPinCode());
			assertEquals(501, document.getServicePointID());
			assertEquals(3, document.getParkingPlaceID());
			assertEquals(102, document.getPermStateID());
			assertEquals("Kerala", document.getPermStateName());
			assertEquals(402, document.getPermVillageID());
			assertEquals("Kakkanad", document.getPermVillageName());
		}

		@Test
		@DisplayName("a beneficiary with no beneficiary id is left out of the batch")
		void beneficiaryWithNoIdIsLeftOut() {
			// The beneficiary id becomes the Elasticsearch document id, so a row
			// without one cannot be indexed.
			useReflectionForAdvanceSearchRepo();
			Object[] row = projectionRow();
			row[1] = null;
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Arrays.asList(row, projectionRow()));

			assertEquals(1, service.getBeneficiariesBatch(List.of(BEN_REG_ID)).size());
		}

		@Test
		@DisplayName("a beneficiary with a blank beneficiary id is left out of the batch")
		void beneficiaryWithBlankIdIsLeftOut() {
			useReflectionForAdvanceSearchRepo();
			Object[] row = projectionRow();
			row[1] = "";
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(row));

			assertTrue(service.getBeneficiariesBatch(List.of(BEN_REG_ID)).isEmpty());
		}

		@Test
		@DisplayName("one malformed row does not lose the rest of the page")
		void oneMalformedRowDoesNotLoseThePage() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Arrays.asList(new Object[] { 1L }, projectionRow()));

			assertEquals(1, service.getBeneficiariesBatch(List.of(BEN_REG_ID)).size());
		}

		@Test
		@DisplayName("a failing projection query yields no documents rather than propagating")
		void failingProjectionQueryYieldsNoDocuments() {
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenThrow(new IllegalStateException("query timeout"));

			assertTrue(service.getBeneficiariesBatch(List.of(BEN_REG_ID)).isEmpty());
		}

		@ParameterizedTest
		@ValueSource(strings = { "long", "integer", "big-integer", "text", "unparseable" })
		@DisplayName("the numeric column types the native query can return are all handled")
		void everyNumericColumnTypeIsHandled(String kind) {
			useReflectionForAdvanceSearchRepo();
			Object value;
			switch (kind) {
			case "long":
				value = Long.valueOf(101L);
				break;
			case "integer":
				value = Integer.valueOf(101);
				break;
			case "big-integer":
				value = BigInteger.valueOf(101L);
				break;
			case "text":
				value = "101";
				break;
			default:
				value = "not-a-number";
			}
			Object[] row = projectionRow();
			row[20] = value; // stateID
			row[16] = value; // lastModDate
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(row));

			BeneficiaryDocument document = service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0);

			if ("unparseable".equals(kind)) {
				assertNull(document.getStateID());
				assertNull(document.getLastModDate());
			} else {
				assertEquals(101, document.getStateID());
				assertEquals(101L, document.getLastModDate());
			}
		}

		@Test
		@DisplayName("a date column arriving as a plain date or a SQL date is accepted")
		void dateColumnIsAcceptedInEitherForm() {
			useReflectionForAdvanceSearchRepo();
			Object[] withUtilDate = projectionRow();
			withUtilDate[7] = new java.util.Date();
			Object[] withSqlDate = projectionRow();
			withSqlDate[7] = new java.sql.Date(System.currentTimeMillis());
			Object[] withText = projectionRow();
			withText[7] = "1996-01-15";
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Arrays.asList(withUtilDate, withSqlDate, withText));

			List<BeneficiaryDocument> documents = service.getBeneficiariesBatch(List.of(BEN_REG_ID));

			assertNotNull(documents.get(0).getDOB());
			assertNotNull(documents.get(1).getDOB());
			assertNull(documents.get(2).getDOB());
		}
	}

	@Nested
	@DisplayName("ABHA enrichment")
	class AbhaEnrichment {

		@Test
		@DisplayName("a beneficiary's ABHA address and number are attached to its document")
		void abhaDetailsAreAttached() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList()))
					.thenReturn(Collections.singletonList(new Object[] { BigInteger.valueOf(100200300L),
							"asha@abdm", "12-3456-7890-1234", "AADHAAR_OTP",
							Timestamp.valueOf("2026-01-15 10:30:00") }));

			BeneficiaryDocument document = service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0);

			assertEquals("asha@abdm", document.getHealthID());
			assertEquals("12-3456-7890-1234", document.getAbhaID());
			assertNotNull(document.getAbhaCreatedDate());
		}

		@Test
		@DisplayName("only the first ABHA record for a beneficiary is used")
		void onlyTheFirstAbhaRecordIsUsed() {
			// The view returns newest first, and a beneficiary can have more than
			// one ABHA linkage recorded.
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList())).thenReturn(Arrays.asList(
					new Object[] { BigInteger.valueOf(100200300L), "newest@abdm", "11", "OTP", null },
					new Object[] { BigInteger.valueOf(100200300L), "older@abdm", "22", "OTP", null }));

			assertEquals("newest@abdm", service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0).getHealthID());
		}

		@ParameterizedTest
		@ValueSource(strings = { "big-integer", "long", "integer" })
		@DisplayName("the ABHA view's identifier column is matched whichever numeric type it returns")
		void abhaIdentifierColumnIsMatchedWhicheverType(String kind) {
			useReflectionForAdvanceSearchRepo();
			Object id;
			switch (kind) {
			case "long":
				id = Long.valueOf(100200300L);
				break;
			case "integer":
				id = Integer.valueOf(100200300);
				break;
			default:
				id = BigInteger.valueOf(100200300L);
			}
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList()))
					.thenReturn(Collections.singletonList(new Object[] { id, "asha@abdm", "12", "OTP", null }));

			assertEquals("asha@abdm", service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0).getHealthID());
		}

		@Test
		@DisplayName("an ABHA row with an unrecognised identifier type is skipped")
		void abhaRowWithUnrecognisedIdentifierIsSkipped() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList())).thenReturn(
					Collections.singletonList(new Object[] { "100200300", "asha@abdm", "12", "OTP", null }));

			assertNull(service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0).getHealthID());
		}

		@Test
		@DisplayName("a failing ABHA query still yields documents, just without ABHA")
		void failingAbhaQueryStillYieldsDocuments() {
			// The ABHA view is not present on every deployment, so its absence
			// must not stop a re-index.
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList()))
					.thenThrow(new IllegalStateException("view does not exist"));

			BeneficiaryDocument document = service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0);

			assertEquals("4001", document.getBenId());
			assertNull(document.getHealthID());
		}

		@Test
		@DisplayName("no ABHA records for the page leaves the documents unenriched")
		void noAbhaRecordsLeavesDocumentsUnenriched() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList())).thenReturn(Collections.emptyList());

			assertNull(service.getBeneficiariesBatch(List.of(BEN_REG_ID)).get(0).getHealthID());
		}
	}

	@Nested
	@DisplayName("single beneficiary loading")
	class SingleBeneficiaryLoading {

		@Test
		@DisplayName("a single beneficiary is loaded through the same batch query")
		void singleBeneficiaryUsesTheBatchQuery() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));

			assertEquals("4001", service.getBeneficiaryFromDatabase(BEN_REG_ID).getBenId());
		}

		@Test
		@DisplayName("an unknown beneficiary yields no document")
		void unknownBeneficiaryYieldsNoDocument() {
			when(mappingRepo.findCompleteDataByBenRegIds(anyList())).thenReturn(Collections.emptyList());

			assertNull(service.getBeneficiaryFromDatabase(BEN_REG_ID));
			assertNull(service.getBeneficiaryWithAbhaDetails(BEN_REG_ID));
		}

		@Test
		@DisplayName("a null identifier is rejected without a query")
		void nullIdentifierIsRejected() {
			assertNull(service.getBeneficiaryWithAbhaDetails(null));

			verify(mappingRepo, never()).findCompleteDataByBenRegIds(anyList());
		}

		@Test
		@DisplayName("the real-time sync path returns the beneficiary with its ABHA details")
		void realTimeSyncPathReturnsAbhaDetails() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList())).thenReturn(Collections
					.singletonList(new Object[] { BigInteger.valueOf(100200300L), "asha@abdm", "12", "OTP", null }));

			BeneficiaryDocument document = service.getBeneficiaryWithAbhaDetails(BEN_REG_ID);

			assertEquals("asha@abdm", document.getHealthID());
		}

		@Test
		@DisplayName("a beneficiary with no ABHA is still returned by the real-time sync path")
		void beneficiaryWithNoAbhaIsStillReturned() {
			useReflectionForAdvanceSearchRepo();
			when(mappingRepo.findCompleteDataByBenRegIds(anyList()))
					.thenReturn(Collections.singletonList(projectionRow()));
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegIDs(anyList())).thenReturn(Collections.emptyList());

			assertNotNull(service.getBeneficiaryWithAbhaDetails(BEN_REG_ID));
		}
	}
}
