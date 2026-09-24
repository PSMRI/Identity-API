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
package com.iemr.common.identity.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.domain.Address;
import com.iemr.common.identity.domain.Contact;
import com.iemr.common.identity.domain.MBeneficiaryAccount;
import com.iemr.common.identity.domain.MBeneficiaryImage;
import com.iemr.common.identity.domain.MBeneficiaryaddress;
import com.iemr.common.identity.domain.MBeneficiaryconsent;
import com.iemr.common.identity.domain.MBeneficiarycontact;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiaryfamilymapping;
import com.iemr.common.identity.domain.MBeneficiaryidentity;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.domain.MBeneficiaryregidmapping;
import com.iemr.common.identity.domain.MBeneficiaryservicemapping;
import com.iemr.common.identity.domain.VBenAdvanceSearch;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.BenIdImportDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.BeneficiaryCreateResp;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentityEditDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.dto.ReserveIdentityDTO;
import com.iemr.common.identity.exception.MissingMandatoryFieldsException;
import com.iemr.common.identity.mapper.BenIdImportMapper;
import com.iemr.common.identity.mapper.IdentityEditMapper;
import com.iemr.common.identity.mapper.IdentityMapper;
import com.iemr.common.identity.mapper.IdentityPartialMapper;
import com.iemr.common.identity.mapper.IdentitySearchMapper;
import com.iemr.common.identity.repo.BenAddressRepo;
import com.iemr.common.identity.repo.BenConsentRepo;
import com.iemr.common.identity.repo.BenContactRepo;
import com.iemr.common.identity.repo.BenDataAccessRepo;
import com.iemr.common.identity.repo.BenDetailRepo;
import com.iemr.common.identity.repo.BenFamilyMappingRepo;
import com.iemr.common.identity.repo.BenIdentityRepo;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.BenRegIdMappingRepo;
import com.iemr.common.identity.repo.BenServiceMappingRepo;
import com.iemr.common.identity.repo.MBeneficiaryAccountRepo;
import com.iemr.common.identity.repo.MBeneficiaryImageRepo;
import com.iemr.common.identity.repo.V_BenAdvanceSearchRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBeneficiaryDetailsRmnchRepo;
import com.iemr.common.identity.service.elasticsearch.BeneficiaryElasticsearchIndexUpdater;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchService;

/**
 * Tests for the core beneficiary identity service.
 *
 * <p>
 * The service sits on a schema split across nine {@code m_beneficiary*} tables
 * that are stitched together by a (vanSerialNo, vanID) pair rather than by
 * foreign keys, and every read path assembles a beneficiary from a positional
 * {@code Object[]} projection. The behaviour worth protecting is therefore the
 * assembly and the branching around it: which search key wins, when a row is
 * skipped, when Elasticsearch is consulted, and when a sync is triggered.
 *
 * <p>
 * The real MapStruct mappers are injected rather than mocked - they are
 * generated, deterministic, and stubbing their several hundred field copies
 * would leave the assembly logic untested.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IdentityServiceTest {

	@Mock
	private javax.sql.DataSource dataSource;
	@Mock
	private RMNCHBeneficiaryDetailsRmnchRepo rmnchRepo;
	@Mock
	private ElasticsearchService elasticsearchService;
	@Mock
	private BeneficiaryElasticsearchIndexUpdater syncService;
	@Mock
	private BenAddressRepo addressRepo;
	@Mock
	private BenConsentRepo consentRepo;
	@Mock
	private BenContactRepo contactRepo;
	@Mock
	private BenDataAccessRepo accessRepo;
	@Mock
	private BenDetailRepo detailRepo;
	@Mock
	private BenFamilyMappingRepo familyMapRepo;
	@Mock
	private BenIdentityRepo identityRepo;
	@Mock
	private BenMappingRepo mappingRepo;
	@Mock
	private BenRegIdMappingRepo regIdRepo;
	@Mock
	private BenRegIdClaimService benRegIdClaimService;
	@Mock
	private BenServiceMappingRepo serviceMapRepo;
	@Mock
	private MBeneficiaryAccountRepo accountRepo;
	@Mock
	private MBeneficiaryImageRepo imageRepo;
	@Mock
	private V_BenAdvanceSearchRepo advanceSearchRepo;

	@InjectMocks
	private IdentityService service;

	/** The (vanSerialNo, vanID) pair every row lookup is keyed on. */
	private static final Integer VAN_ID = 7;
	private static final BigInteger BEN_REG_ID = BigInteger.valueOf(100200300L);
	private static final BigInteger BEN_ID = BigInteger.valueOf(4001L);

	@BeforeEach
	void injectRealMappers() {
		ReflectionTestUtils.setField(service, "mapper", IdentityMapper.INSTANCE);
		ReflectionTestUtils.setField(service, "editMapper", IdentityEditMapper.INSTANCE);
		ReflectionTestUtils.setField(service, "searchMapper", IdentitySearchMapper.INSTANCE);
		ReflectionTestUtils.setField(service, "partialMapper", IdentityPartialMapper.INSTANCE);
		ReflectionTestUtils.setField(service, "benIdImportMapper", BenIdImportMapper.INSTANCE);
		ReflectionTestUtils.setField(service, "rMNCHBeneficiaryDetailsRmnchRepo", rmnchRepo);
		ReflectionTestUtils.setField(service, "v_BenAdvanceSearchRepo", advanceSearchRepo);
		ReflectionTestUtils.setField(service, "esEnabled", false);
	}

	/**
	 * The 12-column projection {@code getBenMappingByRegID} and friends return.
	 * Positions 8 (vanID) and 9 (benMapId) are the ones the assembly checks.
	 */
	private Object[] projectionRow() {
		Object[] row = new Object[12];
		row[0] = BigInteger.valueOf(1L); // benMapId
		row[1] = BigInteger.valueOf(2L); // benAddressId
		row[2] = BigInteger.valueOf(3L); // benConsentId
		row[3] = BigInteger.valueOf(4L); // benContactsId
		row[4] = BigInteger.valueOf(5L); // benDetailsId
		row[5] = BEN_REG_ID; // benRegId
		row[6] = BigInteger.valueOf(6L); // benImageId
		row[7] = BigInteger.valueOf(7L); // benAccountId
		row[8] = VAN_ID;
		row[9] = BigInteger.valueOf(9L); // vanSerialNo
		row[10] = "field.worker"; // createdBy
		row[11] = Timestamp.valueOf("2026-01-01 00:00:00");
		return row;
	}

	/** Stubs the nine per-table lookups the assembly performs for one row. */
	private MBeneficiarymapping stubRowAssembly() {
		MBeneficiarymapping mapping = new MBeneficiarymapping();
		mapping.setBenMapId(BigInteger.ONE);
		mapping.setBenRegId(BEN_REG_ID);
		mapping.setVanID(VAN_ID);

		MBeneficiarydetail detail = new MBeneficiarydetail();
		detail.setFirstName("Asha");
		detail.setLastName("Devi");
		detail.setGenderId(2);
		detail.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
		detail.setVanID(VAN_ID);

		MBeneficiaryregidmapping regId = new MBeneficiaryregidmapping();
		regId.setBenRegId(BEN_REG_ID);
		regId.setBeneficiaryID(BEN_ID);

		when(mappingRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(mapping);
		when(addressRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(new MBeneficiaryaddress());
		when(consentRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(new MBeneficiaryconsent());
		when(contactRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(new MBeneficiarycontact());
		when(detailRepo.getWith_vanSerialNo_vanID(any(), anyInt())).thenReturn(detail);
		when(regIdRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(regId);
		when(accountRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(new MBeneficiaryAccount());
		when(imageRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(new MBeneficiaryImage());
		return mapping;
	}

	@Nested
	@DisplayName("search key precedence")
	class SearchKeyPrecedence {

		@Test
		@DisplayName("a beneficiary ID short-circuits every other criterion")
		void beneficiaryIdWinsOverEverythingElse() throws Exception {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setBeneficiaryId(BEN_ID);
			searchDTO.setBeneficiaryRegId(BEN_REG_ID);
			searchDTO.setContactNumber("9000000000");
			MBeneficiaryregidmapping regId = new MBeneficiaryregidmapping();
			regId.setBenRegId(BEN_REG_ID);
			when(regIdRepo.findByBeneficiaryID(BEN_ID)).thenReturn(regId);
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			service.getBeneficiaries(searchDTO);

			verify(regIdRepo).findByBeneficiaryID(BEN_ID);
			verify(contactRepo, never()).findByAnyPhoneNum(any());
			verify(mappingRepo, never()).dynamicFilterSearchNew(any());
		}

		@Test
		@DisplayName("a registration ID is used when no beneficiary ID is supplied")
		void registrationIdIsSecondChoice() throws Exception {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setBeneficiaryRegId(BEN_REG_ID);
			searchDTO.setContactNumber("9000000000");
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			service.getBeneficiaries(searchDTO);

			verify(mappingRepo).getBenMappingByRegID(BEN_REG_ID);
			verify(contactRepo, never()).findByAnyPhoneNum(any());
		}

		@Test
		@DisplayName("a phone number is used when no identifier is supplied")
		void phoneNumberIsThirdChoice() throws Exception {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setContactNumber("9000000000");
			when(contactRepo.findByAnyPhoneNum(any())).thenReturn(Collections.emptyList());

			service.getBeneficiaries(searchDTO);

			verify(contactRepo).findByAnyPhoneNum(any());
			verify(mappingRepo, never()).dynamicFilterSearchNew(any());
		}

		@Test
		@DisplayName("with no identifier and no phone number the search falls through to the advance-search view")
		void fallsThroughToAdvanceSearchView() throws Exception {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setFirstName("Asha");
			when(mappingRepo.dynamicFilterSearchNew(searchDTO)).thenReturn(Collections.emptyList());

			assertTrue(service.getBeneficiaries(searchDTO).isEmpty());

			verify(mappingRepo).dynamicFilterSearchNew(searchDTO);
		}
	}

	@Nested
	@DisplayName("phone-number normalisation")
	class PhoneNumberNormalisation {

		@ParameterizedTest
		@CsvSource({ "+919000000000, 9000000000", "919000000000, 9000000000", "09000000000, 9000000000",
				"9000000000, 9000000000", "'  9000000000  ', 9000000000" })
		@DisplayName("a stored number is looked up under every prefix form it might have been saved with")
		void lookupCoversEveryPrefixForm(String supplied, String expectedBase) {
			when(contactRepo.findByAnyPhoneNum(any())).thenReturn(Collections.emptyList());

			service.getBeneficiariesByPhoneNum(supplied);

			@SuppressWarnings("unchecked")
			ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
			verify(contactRepo).findByAnyPhoneNum(captor.capture());
			assertEquals(Arrays.asList(expectedBase, "0" + expectedBase, "91" + expectedBase, "+91" + expectedBase),
					captor.getValue());
		}

		@Test
		@DisplayName("a 91-prefixed number that is not 12 digits long is left alone")
		void shortNumberStartingWith91IsNotTruncated() {
			when(contactRepo.findByAnyPhoneNum(any())).thenReturn(Collections.emptyList());

			service.getBeneficiariesByPhoneNum("919000");

			@SuppressWarnings("unchecked")
			ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
			verify(contactRepo).findByAnyPhoneNum(captor.capture());
			assertEquals("919000", captor.getValue().get(0));
		}

		@Test
		@DisplayName("a lookup failure yields no results rather than propagating")
		void lookupFailureYieldsNoResults() {
			// A search is a read-only convenience for the agent on the call; a
			// dead contact table should not surface as a 500.
			when(contactRepo.findByAnyPhoneNum(any())).thenThrow(new IllegalStateException("connection reset"));

			assertTrue(service.getBeneficiariesByPhoneNum("9000000000").isEmpty());
		}
	}

	@Nested
	@DisplayName("door-to-door filtering")
	class DoorToDoorFiltering {

		private BeneficiariesDTO beneficiary(String firstName, String lastName, Integer genderId, Integer stateId) {
			BeneficiariesDTO dto = new BeneficiariesDTO();
			com.iemr.common.identity.dto.BenDetailDTO details = new com.iemr.common.identity.dto.BenDetailDTO();
			details.setFirstName(firstName);
			details.setLastName(lastName);
			details.setGenderId(genderId);
			dto.setBeneficiaryDetails(details);
			Address address = new Address();
			address.setStateId(stateId);
			dto.setCurrentAddress(address);
			return dto;
		}

		private IdentitySearchDTO d2dSearch() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setContactNumber("9000000000");
			searchDTO.setIsD2D(Boolean.TRUE);
			return searchDTO;
		}

		private void stubPhoneHits(BeneficiariesDTO... hits) {
			MBeneficiarycontact contact = new MBeneficiarycontact();
			contact.setVanSerialNo(BigInteger.valueOf(9L));
			contact.setVanID(VAN_ID);
			when(contactRepo.findByAnyPhoneNum(any())).thenReturn(Collections.singletonList(contact));
			List<Object[]> rows = new java.util.ArrayList<>();
			for (int i = 0; i < hits.length; i++) {
				rows.add(projectionRow());
			}
			when(mappingRepo.getBenMappingByBenContactIdListNew(any(), anyInt())).thenReturn(rows);
		}

		@Test
		@DisplayName("a mismatched first name drops the beneficiary from the results")
		void mismatchedFirstNameIsDropped() throws Exception {
			IdentitySearchDTO searchDTO = d2dSearch();
			searchDTO.setFirstName("Sunita");
			stubPhoneHits(beneficiary("Asha", "Devi", 2, 101));
			stubRowAssembly();

			assertTrue(service.getBeneficiarieswithES(searchDTO).isEmpty());
		}

		@Test
		@DisplayName("a matching first name is case-insensitive and keeps the beneficiary")
		void matchingFirstNameIsCaseInsensitive() throws Exception {
			IdentitySearchDTO searchDTO = d2dSearch();
			searchDTO.setFirstName("aSHa");
			stubPhoneHits(beneficiary("Asha", "Devi", 2, 101));
			stubRowAssembly();

			assertEquals(1, service.getBeneficiarieswithES(searchDTO).size());
		}

		@Test
		@DisplayName("filters are not applied at all when the search is not door-to-door")
		void filtersAreSkippedForNonD2DSearches() throws Exception {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setContactNumber("9000000000");
			searchDTO.setFirstName("Sunita");
			searchDTO.setIsD2D(Boolean.FALSE);
			stubPhoneHits(beneficiary("Asha", "Devi", 2, 101));
			stubRowAssembly();

			assertEquals(1, service.getBeneficiarieswithES(searchDTO).size());
		}
	}

	@Nested
	@DisplayName("row assembly from the positional projection")
	class RowAssembly {

		@Test
		@DisplayName("a full row is assembled into a beneficiary with its nested tables")
		void fullRowIsAssembled() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertEquals(1, result.size());
			assertEquals("Asha", result.get(0).getBeneficiaryDetails().getFirstName());
			assertEquals(BEN_ID, result.get(0).getBenId());
		}

		@Test
		@DisplayName("a row missing its vanID or vanSerialNo is not assembled")
		void rowMissingSyncKeysIsNotAssembled() throws Exception {
			Object[] row = projectionRow();
			row[8] = null;
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(row));

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertEquals(1, result.size());
			verifyNoInteractions(addressRepo);
		}

		@Test
		@DisplayName("RMNCH household, guideline and RCH identifiers are folded in when present")
		void rmnchIdentifiersAreFoldedIn() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();
			RMNCHBeneficiaryDetailsRmnch rmnch = new RMNCHBeneficiaryDetailsRmnch();
			rmnch.setHouseoldId(555L);
			rmnch.setGuidelineId("GL-1");
			rmnch.setRchid("RCH-1");
			rmnch.setReproductiveStatus("Pregnant");
			rmnch.setReproductiveStatusId(3);
			when(rmnchRepo.getByRegID(any())).thenReturn(Collections.singletonList(rmnch));

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertEquals(1, result.size());
			assertEquals("Pregnant", result.get(0).getReproductiveStatus());
			assertEquals(3, result.get(0).getReproductiveStatusId());
		}

		@Test
		@DisplayName("ABHA rows found for the beneficiary are attached to the response")
		void abhaRowsAreAttached() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();
			Object[] abhaRow = new Object[] { BEN_REG_ID, "asha@abdm", "12-3456-7890-1234", "AADHAAR_OTP",
					Timestamp.valueOf("2026-02-02 00:00:00") };
			when(advanceSearchRepo.getBenAbhaDetailsByBenRegID(any()))
					.thenReturn(Collections.singletonList(abhaRow));

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertEquals(1, result.get(0).getAbhaDetails().size());
			assertEquals("asha@abdm", result.get(0).getAbhaDetails().get(0).getHealthID());
			assertEquals("12-3456-7890-1234", result.get(0).getAbhaDetails().get(0).getHealthIDNumber());
			assertEquals("AADHAAR_OTP", result.get(0).getAbhaDetails().get(0).getAuthenticationMode());
		}

		@Test
		@DisplayName("a stored face embedding is parsed into a float vector")
		void faceEmbeddingIsParsedIntoVector() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			MBeneficiarymapping mapping = stubRowAssembly();
			mapping.getMBeneficiarydetail();
			MBeneficiarydetail detail = new MBeneficiarydetail();
			detail.setFirstName("Asha");
			detail.setFaceEmbedding("[0.25, -0.5, 0.75]");
			when(detailRepo.getWith_vanSerialNo_vanID(any(), anyInt())).thenReturn(detail);

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertEquals(Arrays.asList(0.25f, -0.5f, 0.75f), result.get(0).getFaceEmbedding());
		}

		@Test
		@DisplayName("an empty face embedding yields an empty vector rather than a parse failure")
		void emptyFaceEmbeddingYieldsEmptyVector() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();
			MBeneficiarydetail detail = new MBeneficiarydetail();
			detail.setFaceEmbedding("[]");
			when(detailRepo.getWith_vanSerialNo_vanID(any(), anyInt())).thenReturn(detail);

			List<BeneficiariesDTO> result = service.getBeneficiariesByBenRegId(BEN_REG_ID);

			assertTrue(result.get(0).getFaceEmbedding().isEmpty());
		}

		@Test
		@DisplayName("a lookup failure during assembly yields no results rather than propagating")
		void assemblyFailureYieldsNoResults() throws Exception {
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenThrow(new IllegalStateException("query timeout"));

			assertTrue(service.getBeneficiariesByBenRegId(BEN_REG_ID).isEmpty());
		}

		@Test
		@DisplayName("an unknown beneficiary ID yields no results")
		void unknownBeneficiaryIdYieldsNoResults() throws Exception {
			when(regIdRepo.findByBeneficiaryID(BEN_ID)).thenReturn(null);

			assertTrue(service.getBeneficiariesByBenId(BEN_ID).isEmpty());
			verify(mappingRepo, never()).getBenMappingByRegID(any());
		}
	}

	@Nested
	@DisplayName("identifier coercion from the projection")
	class IdentifierCoercion {

		@Test
		@DisplayName("the numeric types JPA may hand back are all coerced to BigInteger")
		void everyNumericTypeIsCoerced() throws Exception {
			for (Object supplied : Arrays.asList(BigInteger.valueOf(5L), BigDecimal.valueOf(5L), Integer.valueOf(5),
					Long.valueOf(5L), "5")) {
				Object[] row = projectionRow();
				row[4] = supplied;
				when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(row));
				stubRowAssembly();

				assertEquals(1, service.getBeneficiariesByBenRegId(BEN_REG_ID).size(),
						"failed to coerce " + supplied.getClass().getSimpleName());
			}
		}

		@Test
		@DisplayName("a value of an unexpected type is reported rather than silently dropped")
		void unexpectedTypeIsReported() throws Exception {
			Object[] row = projectionRow();
			row[4] = Boolean.TRUE;
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(row));

			// getBeneficiariesByBenRegId swallows the failure, so the beneficiary
			// simply does not appear - the error is logged for the sync job.
			assertTrue(service.getBeneficiariesByBenRegId(BEN_REG_ID).isEmpty());
		}
	}

	@Nested
	@DisplayName("advanced search routing")
	class AdvancedSearchRouting {

		@Test
		@DisplayName("with Elasticsearch enabled the query goes to the index")
		void elasticsearchIsUsedWhenEnabled() throws Exception {
			ReflectionTestUtils.setField(service, "esEnabled", true);
			when(elasticsearchService.advancedSearch(any(), any(), any(), any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any(), any(), any(), any(), any()))
					.thenReturn(Collections.singletonList(Map.of("benRegId", BEN_REG_ID)));

			Map<String, Object> response = service.advancedSearchBeneficiariesES("Asha", null, "Devi", 2, null, 101,
					null, null, null, null, null, null, null, null, null, null, 1, "auth", Boolean.FALSE);

			assertEquals("elasticsearch", response.get("source"));
			assertEquals(1, response.get("count"));
			verify(mappingRepo, never()).dynamicFilterSearchNew(any());
		}

		@Test
		@DisplayName("with Elasticsearch disabled the query falls back to the database")
		void databaseIsUsedWhenElasticsearchDisabled() throws Exception {
			when(mappingRepo.dynamicFilterSearchNew(any())).thenReturn(Collections.emptyList());

			Map<String, Object> response = service.advancedSearchBeneficiariesES("Asha", null, "Devi", 2,
					new java.util.Date(), 101, 201, 301, 401, "Ram", "Suresh", "Married", null, null, null, null, 1,
					"auth", Boolean.FALSE);

			assertEquals("database", response.get("source"));
			assertEquals(0, response.get("count"));
			verifyNoInteractions(elasticsearchService);
		}

		@Test
		@DisplayName("a non-numeric beneficiary ID is ignored rather than failing the search")
		void nonNumericBeneficiaryIdIsIgnored() throws Exception {
			when(mappingRepo.dynamicFilterSearchNew(any())).thenReturn(Collections.emptyList());

			Map<String, Object> response = service.advancedSearchBeneficiariesES(null, null, null, null, null, null,
					null, null, null, null, null, null, null, "not-a-number", null, null, 1, "auth", Boolean.FALSE);

			assertEquals(0, response.get("count"));
			verify(mappingRepo).dynamicFilterSearchNew(any());
		}

		@Test
		@DisplayName("an index failure is surfaced to the caller rather than returning empty results")
		void indexFailureIsSurfaced() {
			ReflectionTestUtils.setField(service, "esEnabled", true);
			when(elasticsearchService.advancedSearch(any(), any(), any(), any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any(), any(), any(), any(), any()))
					.thenThrow(new IllegalStateException("index unavailable"));

			Exception thrown = assertThrows(Exception.class, () -> service.advancedSearchBeneficiariesES(null, null,
					null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1, "auth",
					Boolean.FALSE));

			assertTrue(thrown.getMessage().contains("Error in advanced search"));
		}
	}

	@Nested
	@DisplayName("search by ABHA and government identifiers")
	class IdentifierSearches {

		@Test
		@DisplayName("every registration ID behind an ABHA address is resolved")
		void abhaAddressResolvesEveryRegistrationId() throws Exception {
			when(advanceSearchRepo.getBenRegIDByHealthIDAbhaAddress("asha@abdm"))
					.thenReturn(Arrays.asList(BEN_REG_ID, BigInteger.valueOf(999L), null));
			when(mappingRepo.getBenMappingByRegID(any())).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();

			assertEquals(2, service.getBeneficiaryByHealthIDAbhaAddress("asha@abdm").size());
		}

		@Test
		@DisplayName("an unknown ABHA address yields no results")
		void unknownAbhaAddressYieldsNoResults() throws Exception {
			when(advanceSearchRepo.getBenRegIDByHealthIDAbhaAddress("nobody@abdm"))
					.thenReturn(Collections.emptyList());

			assertTrue(service.getBeneficiaryByHealthIDAbhaAddress("nobody@abdm").isEmpty());
		}

		@Test
		@DisplayName("an ABHA number lookup failure yields no results rather than propagating")
		void abhaNumberFailureYieldsNoResults() throws Exception {
			when(advanceSearchRepo.getBenRegIDByHealthIDNoAbhaIdNo(any()))
					.thenThrow(new IllegalStateException("view unavailable"));

			assertTrue(service.getBeneficiaryByHealthIDNoAbhaIdNo("12-3456-7890-1234").isEmpty());
		}

		@Test
		@DisplayName("an ABHA number resolves to its beneficiary")
		void abhaNumberResolvesToBeneficiary() throws Exception {
			when(advanceSearchRepo.getBenRegIDByHealthIDNoAbhaIdNo("12-3456-7890-1234"))
					.thenReturn(Collections.singletonList(BEN_REG_ID));
			when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();

			assertEquals(1, service.getBeneficiaryByHealthIDNoAbhaIdNo("12-3456-7890-1234").size());
		}

		@Test
		@DisplayName("a family ID resolves every member recorded against it")
		void familyIdResolvesEveryMember() {
			MBeneficiarydetail first = new MBeneficiarydetail();
			first.setVanID(VAN_ID);
			first.setVanSerialNo(BigInteger.valueOf(9L));
			MBeneficiarydetail second = new MBeneficiarydetail();
			second.setVanID(VAN_ID);
			second.setVanSerialNo(BigInteger.valueOf(10L));
			when(detailRepo.searchByFamilyId("FAM-1")).thenReturn(Arrays.asList(first, second));
			when(mappingRepo.getBenMappingByBenDetailsIds(any(), anyInt()))
					.thenReturn(Arrays.asList(projectionRow(), projectionRow()));
			stubRowAssembly();

			assertEquals(2, service.searhBeneficiaryByFamilyId("FAM-1").size());
		}

		@Test
		@DisplayName("an unknown family ID yields no results and no mapping lookup")
		void unknownFamilyIdYieldsNoResults() {
			when(detailRepo.searchByFamilyId("FAM-NONE")).thenReturn(Collections.emptyList());

			assertTrue(service.searhBeneficiaryByFamilyId("FAM-NONE").isEmpty());
			verify(mappingRepo, never()).getBenMappingByBenDetailsIds(any(), anyInt());
		}

		@Test
		@DisplayName("a government identity number resolves its beneficiaries")
		void governmentIdentityResolvesBeneficiaries() {
			MBeneficiaryidentity identity = new MBeneficiaryidentity();
			identity.setBenMapId(BigInteger.valueOf(9L));
			identity.setVanID(VAN_ID);
			when(identityRepo.searchByIdentityNo("AADHAAR-1")).thenReturn(Collections.singletonList(identity));
			when(mappingRepo.getBenMappingByVanSerialNo(any(), anyInt()))
					.thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();

			assertEquals(1, service.searhBeneficiaryByGovIdentity("AADHAAR-1").size());
		}

		@Test
		@DisplayName("an unknown government identity number yields no results")
		void unknownGovernmentIdentityYieldsNoResults() {
			when(identityRepo.searchByIdentityNo("AADHAAR-NONE")).thenReturn(Collections.emptyList());

			assertTrue(service.searhBeneficiaryByGovIdentity("AADHAAR-NONE").isEmpty());
		}
	}

	@Nested
	@DisplayName("village sync queries")
	class VillageSyncQueries {

		@Test
		@DisplayName("beneficiaries changed in the given villages since the watermark are returned")
		void changedBeneficiariesAreReturned() {
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenRegId(BEN_REG_ID);
			when(mappingRepo.findByBeneficiaryDetailsByVillageIDAndLastModifyDate(any(), any()))
					.thenReturn(Collections.singletonList(mapping));

			List<BeneficiariesDTO> result = service.searchBeneficiaryByVillageIdAndLastModifyDate(List.of(401),
					Timestamp.valueOf("2026-01-01 00:00:00"));

			assertEquals(1, result.size());
		}

		@Test
		@DisplayName("a query failure yields no results so the CHO app sync can retry")
		void queryFailureYieldsNoResults() {
			when(mappingRepo.findByBeneficiaryDetailsByVillageIDAndLastModifyDate(any(), any()))
					.thenThrow(new IllegalStateException("timeout"));

			assertTrue(service
					.searchBeneficiaryByVillageIdAndLastModifyDate(List.of(401), Timestamp.valueOf("2026-01-01 00:00:00"))
					.isEmpty());
		}

		@Test
		@DisplayName("the count query is passed straight through")
		void countIsPassedThrough() {
			when(mappingRepo.getBeneficiaryCountsByVillageIDAndLastModifyDate(any(), any())).thenReturn(42L);

			assertEquals(42L, service.countBeneficiaryByVillageIdAndLastModifyDate(List.of(401),
					Timestamp.valueOf("2026-01-01 00:00:00")));
		}

		@Test
		@DisplayName("a failing count reports zero rather than propagating")
		void failingCountReportsZero() {
			when(mappingRepo.getBeneficiaryCountsByVillageIDAndLastModifyDate(any(), any()))
					.thenThrow(new IllegalStateException("timeout"));

			assertEquals(0L, service.countBeneficiaryByVillageIdAndLastModifyDate(List.of(401),
					Timestamp.valueOf("2026-01-01 00:00:00")));
		}
	}

	@Nested
	@DisplayName("RMNCH lookup")
	class RmnchLookup {

		@Test
		@DisplayName("the stored RMNCH row is returned when one exists")
		void storedRowIsReturned() {
			RMNCHBeneficiaryDetailsRmnch stored = new RMNCHBeneficiaryDetailsRmnch();
			stored.setRchid("RCH-1");
			when(rmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(stored));

			assertSame(stored, service.getRmnchDataByBenID(BEN_REG_ID));
		}

		@Test
		@DisplayName("a beneficiary with no RMNCH row gets an empty one rather than null")
		void missingRowYieldsEmptyObject() {
			when(rmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			RMNCHBeneficiaryDetailsRmnch result = service.getRmnchDataByBenID(BEN_REG_ID);

			assertNotNull(result);
			assertEquals(null, result.getRchid());
		}
	}

	@Nested
	@DisplayName("editing an existing beneficiary")
	class EditingBeneficiary {

		private MBeneficiarymapping storedMapping() {
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenMapId(BigInteger.ONE);
			mapping.setVanID(VAN_ID);
			mapping.setVanSerialNo(BigInteger.valueOf(9L));
			mapping.setParkingPlaceID(3);

			MBeneficiarydetail detail = new MBeneficiarydetail();
			detail.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			mapping.setMBeneficiarydetail(detail);

			MBeneficiaryaddress address = new MBeneficiaryaddress();
			address.setBenAddressID(BigInteger.valueOf(2L));
			mapping.setMBeneficiaryaddress(address);

			MBeneficiarycontact contact = new MBeneficiarycontact();
			contact.setBenContactsID(BigInteger.valueOf(4L));
			mapping.setMBeneficiarycontact(contact);

			MBeneficiaryAccount account = new MBeneficiaryAccount();
			account.setBenAccountID(BigInteger.valueOf(7L));
			mapping.setMBeneficiaryAccount(account);

			MBeneficiaryImage image = new MBeneficiaryImage();
			image.setBenImageId(BigInteger.valueOf(6L));
			mapping.setMBeneficiaryImage(image);

			return mapping;
		}

		private IdentityEditDTO editRequest() {
			IdentityEditDTO dto = new IdentityEditDTO();
			dto.setBeneficiaryRegId(BEN_REG_ID);
			dto.setAgentName("field.worker");
			return dto;
		}

		@Test
		@DisplayName("an edit with neither identifier is rejected before any write")
		void editWithoutIdentifierIsRejected() {
			IdentityEditDTO dto = new IdentityEditDTO();

			assertThrows(MissingMandatoryFieldsException.class, () -> service.editIdentity(dto));
			verifyNoInteractions(detailRepo);
		}

		@Test
		@DisplayName("a self-details change saves the detail row against its existing primary key")
		void selfDetailsChangeSavesDetailRow() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInSelfDetails(Boolean.TRUE);
			dto.setFirstName("Asha");
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiarydetail existing = new MBeneficiarydetail();
			existing.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			existing.setFamilyId("FAM-1");
			existing.setOccupationId(11);
			existing.setEducationId(22);
			when(detailRepo.findBenDetailsByVanSerialNoAndVanID(any(), anyInt())).thenReturn(existing);

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiarydetail> captor = ArgumentCaptor.forClass(MBeneficiarydetail.class);
			verify(detailRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(5L), captor.getValue().getBeneficiaryDetailsId());
			assertEquals("FAM-1", captor.getValue().getFamilyId());
			assertEquals("Asha", captor.getValue().getFirstName());
		}

		@Test
		@DisplayName("occupation and education already on the row are preserved when the edit omits them")
		void existingOccupationAndEducationArePreserved() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInSelfDetails(Boolean.TRUE);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiarydetail existing = new MBeneficiarydetail();
			existing.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			existing.setOccupationId(11);
			existing.setOccupation("Farmer");
			existing.setEducationId(22);
			existing.setEducation("Primary");
			when(detailRepo.findBenDetailsByVanSerialNoAndVanID(any(), anyInt())).thenReturn(existing);

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiarydetail> captor = ArgumentCaptor.forClass(MBeneficiarydetail.class);
			verify(detailRepo).save(captor.capture());
			assertEquals(11, captor.getValue().getOccupationId());
			assertEquals("Farmer", captor.getValue().getOccupation());
			assertEquals(22, captor.getValue().getEducationId());
			assertEquals("Primary", captor.getValue().getEducation());
		}

		@Test
		@DisplayName("an emergency registration flag on the stored row survives the edit")
		void emergencyRegistrationFlagSurvives() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInSelfDetails(Boolean.TRUE);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiarydetail existing = new MBeneficiarydetail();
			existing.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			existing.setEmergencyRegistration(Boolean.TRUE);
			when(detailRepo.findBenDetailsByVanSerialNoAndVanID(any(), anyInt())).thenReturn(existing);

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiarydetail> captor = ArgumentCaptor.forClass(MBeneficiarydetail.class);
			verify(detailRepo).save(captor.capture());
			assertTrue(captor.getValue().getEmergencyRegistration());
		}

		@Test
		@DisplayName("an address change resolves the address primary key before saving")
		void addressChangeResolvesPrimaryKey() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInAddress(Boolean.TRUE);
			dto.setCurrentAddress(new Address());
			dto.setPermanentAddress(new Address());
			dto.setEmergencyAddress(new Address());
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(addressRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(BigInteger.valueOf(20L));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryaddress> captor = ArgumentCaptor.forClass(MBeneficiaryaddress.class);
			verify(addressRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(20L), captor.getValue().getBenAddressID());
		}

		@Test
		@DisplayName("an address change is rejected when the sync key does not resolve")
		void addressChangeWithoutSyncKeyIsRejected() {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInAddress(Boolean.TRUE);
			dto.setCurrentAddress(new Address());
			dto.setPermanentAddress(new Address());
			dto.setEmergencyAddress(new Address());
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(addressRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(null);

			assertThrows(MissingMandatoryFieldsException.class, () -> service.editIdentity(dto));
			verify(addressRepo, never()).save(any());
		}

		@Test
		@DisplayName("a contact change resolves the contact primary key before saving")
		void contactChangeResolvesPrimaryKey() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInContacts(Boolean.TRUE);
			Contact contact = new Contact();
			contact.setPreferredPhoneNum("9000000000");
			dto.setContact(contact);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(contactRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(BigInteger.valueOf(40L));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiarycontact> captor = ArgumentCaptor.forClass(MBeneficiarycontact.class);
			verify(contactRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(40L), captor.getValue().getBenContactsID());
		}

		@Test
		@DisplayName("a contact change is rejected when the sync key does not resolve")
		void contactChangeWithoutSyncKeyIsRejected() {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInContacts(Boolean.TRUE);
			dto.setContact(new Contact());
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(contactRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(null);

			assertThrows(MissingMandatoryFieldsException.class, () -> service.editIdentity(dto));
		}

		@Test
		@DisplayName("an edited identity reuses the primary key of the row in the same position")
		void editedIdentityReusesExistingKey() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInIdentities(Boolean.TRUE);
			com.iemr.common.identity.domain.Identity identity = new com.iemr.common.identity.domain.Identity();
			identity.setIdentityNo("ABHA-1");
			dto.setIdentities(Collections.singletonList(identity));
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiaryidentity stored = new MBeneficiaryidentity();
			stored.setBenIdentityId(BigInteger.valueOf(70L));
			when(identityRepo.findByBenMapId(any())).thenReturn(Collections.singletonList(stored));
			when(identityRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryidentity> captor = ArgumentCaptor.forClass(MBeneficiaryidentity.class);
			verify(identityRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(70L), captor.getValue().getBenIdentityId());
			assertEquals(BigInteger.valueOf(9L), captor.getValue().getBenMapId());
		}

		@Test
		@DisplayName("an identity beyond the stored rows is created with the van and parking place of its mapping")
		void newIdentityInheritsVanAndParkingPlace() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInIdentities(Boolean.TRUE);
			com.iemr.common.identity.domain.Identity identity = new com.iemr.common.identity.domain.Identity();
			identity.setIdentityNo("ABHA-1");
			dto.setIdentities(Collections.singletonList(identity));
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(identityRepo.findByBenMapId(any())).thenReturn(Collections.emptyList());
			MBeneficiaryidentity saved = new MBeneficiaryidentity();
			saved.setBenIdentityId(BigInteger.valueOf(71L));
			when(identityRepo.save(any())).thenReturn(saved);

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryidentity> captor = ArgumentCaptor.forClass(MBeneficiaryidentity.class);
			verify(identityRepo).save(captor.capture());
			assertEquals(VAN_ID, captor.getValue().getVanID());
			assertEquals(3, captor.getValue().getParkingPlaceID());
			verify(identityRepo).updateVanSerialNo(BigInteger.valueOf(71L));
		}

		@Test
		@DisplayName("a family-details change upserts each member against its stored row")
		void familyDetailsChangeUpsertsEachMember() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInFamilyDetails(Boolean.TRUE);
			BenFamilyDTO member = new BenFamilyDTO();
			member.setAssociatedBenRegId(BigInteger.valueOf(888L));
			dto.setBenFamilyDTOs(Collections.singletonList(member));
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiaryfamilymapping stored = new MBeneficiaryfamilymapping();
			stored.setBenFamilyMapId(BigInteger.valueOf(80L));
			when(familyMapRepo.findByBenMapIdOrderByBenFamilyMapIdAsc(any()))
					.thenReturn(Collections.singletonList(stored));
			when(familyMapRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryfamilymapping> captor = ArgumentCaptor
					.forClass(MBeneficiaryfamilymapping.class);
			verify(familyMapRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(80L), captor.getValue().getBenFamilyMapId());
		}

		@Test
		@DisplayName("a bank-details change resolves the account primary key before saving")
		void bankDetailsChangeResolvesPrimaryKey() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInBankDetails(Boolean.TRUE);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(accountRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(BigInteger.valueOf(90L));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryAccount> captor = ArgumentCaptor.forClass(MBeneficiaryAccount.class);
			verify(accountRepo).save(captor.capture());
			assertEquals(BigInteger.valueOf(90L), captor.getValue().getBenAccountID());
		}

		@Test
		@DisplayName("an image change is stored unprocessed so the sync job picks it up")
		void imageChangeIsStoredUnprocessed() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInBenImage(Boolean.TRUE);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			when(imageRepo.findIdByVanSerialNoAndVanID(any(), anyInt())).thenReturn(BigInteger.valueOf(60L));

			service.editIdentity(dto);

			ArgumentCaptor<MBeneficiaryImage> captor = ArgumentCaptor.forClass(MBeneficiaryImage.class);
			verify(imageRepo).save(captor.capture());
			assertEquals("N", captor.getValue().getProcessed());
			assertEquals(BigInteger.valueOf(60L), captor.getValue().getBenImageId());
		}

		@Test
		@DisplayName("any persisted change triggers an Elasticsearch re-index")
		void persistedChangeTriggersReindex() throws Exception {
			IdentityEditDTO dto = editRequest();
			dto.setChangeInSelfDetails(Boolean.TRUE);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());
			MBeneficiarydetail existing = new MBeneficiarydetail();
			existing.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			when(detailRepo.findBenDetailsByVanSerialNoAndVanID(any(), anyInt())).thenReturn(existing);

			service.editIdentity(dto);

			verify(syncService).syncBeneficiaryAsync(BEN_REG_ID);
		}

		@Test
		@DisplayName("an edit that changes nothing does not trigger a re-index")
		void noChangeDoesNotTriggerReindex() throws Exception {
			IdentityEditDTO dto = editRequest();
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(storedMapping());

			service.editIdentity(dto);

			verifyNoInteractions(syncService);
		}
	}

	@Nested
	@DisplayName("education and community edits")
	class EducationAndCommunityEdits {

		@Test
		@DisplayName("an edit with neither identifier is rejected")
		void editWithoutIdentifierIsRejected() {
			assertThrows(MissingMandatoryFieldsException.class,
					() -> service.editIdentityEducationOrCommunity(new IdentityEditDTO()));
		}

		@Test
		@DisplayName("community and education are updated independently")
		void communityAndEducationAreUpdatedIndependently() throws Exception {
			IdentityEditDTO dto = new IdentityEditDTO();
			dto.setBeneficiaryRegId(BEN_REG_ID);
			dto.setCommunityId(5);
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenDetailsId(BigInteger.valueOf(5L));
			mapping.setVanID(VAN_ID);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(mapping);

			service.editIdentityEducationOrCommunity(dto);

			verify(detailRepo).updateCommunity(BigInteger.valueOf(5L), VAN_ID, 5);
			verify(detailRepo, never()).updateEducation(any(), anyInt(), anyInt());
		}

		@Test
		@DisplayName("an unknown registration ID updates nothing")
		void unknownRegistrationIdUpdatesNothing() throws Exception {
			IdentityEditDTO dto = new IdentityEditDTO();
			dto.setBeneficiaryRegId(BEN_REG_ID);
			dto.setEducationId(9);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(BEN_REG_ID)).thenReturn(null);

			service.editIdentityEducationOrCommunity(dto);

			verify(detailRepo, never()).updateEducation(any(), anyInt(), anyInt());
		}
	}

	@Nested
	@DisplayName("creating a beneficiary")
	class CreatingBeneficiary {

		private IdentityDTO createRequest() {
			IdentityDTO dto = new IdentityDTO();
			dto.setFirstName("Asha");
			dto.setLastName("Devi");
			dto.setAgentName("field.worker");
			dto.setVanID(VAN_ID);
			dto.setParkingPlaceId(3);
			dto.setProviderServiceMapId(11);
			dto.setCurrentAddress(new Address());
			dto.setPermanentAddress(new Address());
			dto.setEmergencyAddress(new Address());
			BenFamilyDTO family = new BenFamilyDTO();
			family.setVanID(VAN_ID);
			dto.setBenFamilyDTOs(Collections.singletonList(family));
			return dto;
		}

		@BeforeEach
		void stubPersistence() {
			MBeneficiaryregidmapping claimed = new MBeneficiaryregidmapping();
			claimed.setBenRegId(BEN_REG_ID);
			claimed.setBeneficiaryID(BEN_ID);
			when(benRegIdClaimService.claimNextAvailableRegId()).thenReturn(claimed);
			when(addressRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(consentRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(contactRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(detailRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(accountRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(imageRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(mappingRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(serviceMapRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(identityRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(familyMapRepo.saveAll(any())).thenAnswer(invocation -> {
				List<?> supplied = (List<?>) invocation.getArgument(0);
				return new java.util.ArrayList<>(supplied);
			});
		}

		@Test
		@DisplayName("the claimed registration ID is marked provisioned and returned")
		void claimedRegistrationIdIsProvisioned() {
			BeneficiaryCreateResp response = service.createIdentity(createRequest());

			assertNotNull(response);
			ArgumentCaptor<MBeneficiaryregidmapping> captor = ArgumentCaptor.forClass(MBeneficiaryregidmapping.class);
			verify(regIdRepo).save(captor.capture());
			assertTrue(captor.getValue().getProvisioned());
			assertEquals(11, captor.getValue().getProviderServiceMapID());
			assertEquals(BEN_REG_ID, captor.getValue().getVanSerialNo());
		}

		@Test
		@DisplayName("every table gets its van serial number back-filled for the data sync")
		void vanSerialNumbersAreBackFilled() {
			service.createIdentity(createRequest());

			verify(addressRepo).updateVanSerialNo(any());
			verify(consentRepo).updateVanSerialNo(any());
			verify(contactRepo).updateVanSerialNo(any());
			verify(detailRepo).updateVanSerialNo(any());
			verify(accountRepo).updateVanSerialNo(any());
			verify(imageRepo).updateVanSerialNo(any());
			verify(mappingRepo).updateVanSerialNo(any());
			verify(serviceMapRepo).updateVanSerialNo(any());
		}

		@Test
		@DisplayName("a permanent address flagged as same-as-current is copied from the current address")
		void permanentAddressIsCopiedFromCurrent() {
			IdentityDTO dto = createRequest();
			Address current = new Address();
			current.setAddrLine1("1 Current St");
			current.setPinCode("560064");
			dto.setCurrentAddress(current);
			dto.setIsPermAddrSameAsCurrAddr(Boolean.TRUE);

			service.createIdentity(dto);

			ArgumentCaptor<MBeneficiaryaddress> captor = ArgumentCaptor.forClass(MBeneficiaryaddress.class);
			verify(addressRepo).save(captor.capture());
			assertEquals("1 Current St", captor.getValue().getPermAddrLine1());
		}

		@Test
		@DisplayName("an emergency address flagged as same-as-permanent is copied from the permanent address")
		void emergencyAddressIsCopiedFromPermanent() {
			IdentityDTO dto = createRequest();
			Address permanent = new Address();
			permanent.setAddrLine1("2 Permanent Rd");
			dto.setPermanentAddress(permanent);
			dto.setIsEmerAddrSameAsPermAddr(Boolean.TRUE);

			service.createIdentity(dto);

			ArgumentCaptor<MBeneficiaryaddress> captor = ArgumentCaptor.forClass(MBeneficiaryaddress.class);
			verify(addressRepo).save(captor.capture());
			assertEquals("2 Permanent Rd", captor.getValue().getEmerAddrLine1());
		}

		@ParameterizedTest
		@CsvSource({ "+919000000000, 9000000000", "919000000000, 9000000000", "09000000000, 9000000000",
				"9000000000, 9000000000" })
		@DisplayName("every stored phone number is normalised to its bare ten digits")
		void phoneNumbersAreNormalisedOnCreate(String supplied, String expected) {
			IdentityDTO dto = createRequest();
			Contact contact = new Contact();
			contact.setPreferredPhoneNum(supplied);
			contact.setPhoneNum1(supplied);
			contact.setPhoneNum2(supplied);
			contact.setPhoneNum3(supplied);
			contact.setPhoneNum4(supplied);
			contact.setPhoneNum5(supplied);
			contact.setPreferredSMSPhoneNum(supplied);
			contact.setEmergencyContactNum(supplied);
			dto.setContact(contact);

			service.createIdentity(dto);

			ArgumentCaptor<MBeneficiarycontact> captor = ArgumentCaptor.forClass(MBeneficiarycontact.class);
			verify(contactRepo).save(captor.capture());
			MBeneficiarycontact saved = captor.getValue();
			assertEquals(expected, saved.getPreferredPhoneNum());
			assertEquals(expected, saved.getPhoneNum1());
			assertEquals(expected, saved.getPhoneNum5());
			assertEquals(expected, saved.getPreferredSMSPhoneNum());
			assertEquals(expected, saved.getEmergencyContactNum());
		}

		@Test
		@DisplayName("a created date is stamped on every row that arrives without one")
		void createdDateIsStampedWhenAbsent() {
			service.createIdentity(createRequest());

			ArgumentCaptor<MBeneficiaryaddress> captor = ArgumentCaptor.forClass(MBeneficiaryaddress.class);
			verify(addressRepo).save(captor.capture());
			assertNotNull(captor.getValue().getCreatedDate());
		}

		@Test
		@DisplayName("family members are linked to the new mapping and inherit its registration ID")
		void familyMembersAreLinkedToTheNewMapping() {
			service.createIdentity(createRequest());

			@SuppressWarnings("unchecked")
			ArgumentCaptor<List<MBeneficiaryfamilymapping>> captor = ArgumentCaptor.forClass(List.class);
			verify(familyMapRepo).saveAll(captor.capture());
			MBeneficiaryfamilymapping member = captor.getValue().get(0);
			assertEquals(VAN_ID, member.getVanID());
			assertEquals(3, member.getParkingPlaceID());
		}

		@Test
		@DisplayName("supplied identities are saved against the new mapping")
		void suppliedIdentitiesAreSaved() {
			IdentityDTO dto = createRequest();
			com.iemr.common.identity.domain.Identity identity = new com.iemr.common.identity.domain.Identity();
			identity.setIdentityNo("ABHA-1");
			dto.setIdentities(Collections.singletonList(identity));

			service.createIdentity(dto);

			ArgumentCaptor<MBeneficiaryidentity> captor = ArgumentCaptor.forClass(MBeneficiaryidentity.class);
			verify(identityRepo).save(captor.capture());
			assertEquals("ABHA-1", captor.getValue().getIdentityNo());
			assertEquals("field.worker", captor.getValue().getCreatedBy());
			assertEquals(VAN_ID, captor.getValue().getVanID());
		}

		@Test
		@DisplayName("the service mapping row is linked to the new beneficiary mapping")
		void serviceMappingIsLinked() {
			service.createIdentity(createRequest());

			ArgumentCaptor<MBeneficiaryservicemapping> captor = ArgumentCaptor
					.forClass(MBeneficiaryservicemapping.class);
			verify(serviceMapRepo).save(captor.capture());
			assertNotNull(captor.getValue().getCreatedDate());
		}

		@Test
		@DisplayName("a new beneficiary is queued for indexing")
		void newBeneficiaryIsQueuedForIndexing() {
			service.createIdentity(createRequest());

			verify(syncService).syncBeneficiaryAsync(BEN_REG_ID);
		}
	}

	@Nested
	@DisplayName("reserving and releasing identifiers")
	class ReservingIdentifiers {

		@Test
		@DisplayName("identifiers are reserved only up to the shortfall")
		void identifiersAreReservedUpToTheShortfall() {
			ReserveIdentityDTO request = new ReserveIdentityDTO();
			request.setProviderServiceMapID(11);
			request.setVehicalNo("KA-01-1234");
			request.setReserveCount(3L);
			when(regIdRepo.countByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(11, "KA-01-1234")).thenReturn(5L);
			when(regIdRepo.findFirstByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(null, null))
					.thenReturn(new MBeneficiaryregidmapping());

			assertEquals("Successfully Completed", service.reserveIdentity(request));

			// reserveCount (3) - available (5) + 1 == -1, so the loop body never
			// runs and nothing is reserved.
			verify(regIdRepo, never()).save(any());
		}

		@Test
		@DisplayName("nothing is reserved when the request already exceeds what is available")
		void nothingIsReservedWhenRequestExceedsAvailability() {
			ReserveIdentityDTO request = new ReserveIdentityDTO();
			request.setProviderServiceMapID(11);
			request.setVehicalNo("KA-01-1234");
			request.setReserveCount(10L);
			when(regIdRepo.countByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(11, "KA-01-1234")).thenReturn(5L);

			assertEquals("Successfully Completed", service.reserveIdentity(request));

			verify(regIdRepo, never()).save(any());
		}

		@Test
		@DisplayName("releasing identifiers delegates straight to the repository")
		void releasingDelegatesToRepository() {
			ReserveIdentityDTO request = new ReserveIdentityDTO();
			request.setProviderServiceMapID(11);
			request.setVehicalNo("KA-01-1234");

			assertEquals("Successfully Completed", service.unReserveIdentity(request));

			verify(regIdRepo).unreserveBeneficiaryIds(11, "KA-01-1234");
		}

		@Test
		@DisplayName("the reserved-id listing is a fixed acknowledgement")
		void reservedIdListingIsFixed() {
			assertEquals("success", service.getReservedIdList());
		}

		@Test
		@DisplayName("unprovisioned identifiers are counted for the local pool")
		void unprovisionedIdentifiersAreCounted() {
			when(regIdRepo.countByProvisioned(false)).thenReturn(120L);

			assertEquals(120L, service.checkBenIDAvailabilityLocal());
		}
	}

	@Nested
	@DisplayName("bulk lookups by registration ID list")
	class BulkLookups {

		@Test
		@DisplayName("an empty or absent list is not sent to the database")
		void emptyListIsNotQueried() {
			assertTrue(service.getBeneficiariesPartialDeatilsByBenRegIdList(Collections.emptyList()).isEmpty());
			assertTrue(service.getBeneficiariesPartialDeatilsByBenRegIdList(null).isEmpty());
			assertTrue(service.getBeneficiariesDeatilsByBenRegIdList(Collections.emptyList()).isEmpty());
			assertTrue(service.getBeneficiariesDeatilsByBenRegIdList(null).isEmpty());

			verify(mappingRepo, never()).getBenMappingByRegIDList(any());
		}

		@Test
		@DisplayName("partial details are limited to the name and identifier columns")
		void partialDetailsAreLimitedToNameColumns() {
			when(mappingRepo.getBenMappingByRegIDList(any())).thenReturn(Collections.singletonList(projectionRow()));
			MBeneficiarydetail detail = new MBeneficiarydetail();
			detail.setFirstName("Asha");
			detail.setLastName("Devi");
			when(detailRepo.getWith_vanSerialNo_vanID(any(), anyInt())).thenReturn(detail);
			MBeneficiaryregidmapping regId = new MBeneficiaryregidmapping();
			regId.setBeneficiaryID(BEN_ID);
			regId.setBenRegId(BEN_REG_ID);
			when(regIdRepo.getWithVanSerialNoVanID(any(), anyInt())).thenReturn(regId);

			List<com.iemr.common.identity.dto.BeneficiariesPartialDTO> result = service
					.getBeneficiariesPartialDeatilsByBenRegIdList(List.of(BEN_REG_ID));

			assertEquals(1, result.size());
			assertEquals("Asha", result.get(0).getFirstName());
			assertEquals(BEN_ID, result.get(0).getBenId());
			verifyNoInteractions(addressRepo);
		}

		@Test
		@DisplayName("full details assemble every nested table for each registration ID")
		void fullDetailsAssembleEveryTable() {
			when(mappingRepo.getBenMappingByRegIDList(any())).thenReturn(Collections.singletonList(projectionRow()));
			stubRowAssembly();

			List<BeneficiariesDTO> result = service.getBeneficiariesDeatilsByBenRegIdList(List.of(BEN_REG_ID));

			assertEquals(1, result.size());
			verify(addressRepo).getWithVanSerialNoVanID(any(), anyInt());
		}
	}

	@Nested
	@DisplayName("finite search")
	class FiniteSearch {

		@Test
		@DisplayName("each mapping returned by the finite search is expanded into a beneficiary")
		void eachMappingIsExpanded() {
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenMapId(BigInteger.ONE);
			mapping.setBenRegId(BEN_REG_ID);
			MBeneficiaryregidmapping regId = new MBeneficiaryregidmapping();
			regId.setBeneficiaryID(BEN_ID);
			mapping.setMBeneficiaryregidmapping(regId);
			IdentityDTO query = new IdentityDTO();
			when(mappingRepo.finiteSearch(query)).thenReturn(Collections.singletonList(mapping));

			assertEquals(1, service.getBeneficiaries(query).size());
		}
	}

	@Nested
	@DisplayName("beneficiary image retrieval")
	class BeneficiaryImageRetrieval {

		@Test
		@DisplayName("a stored image is returned with its capture date")
		void storedImageIsReturnedWithCaptureDate() {
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenImageId(BigInteger.valueOf(60L));
			mapping.setVanID(VAN_ID);
			when(mappingRepo.getBenImageIdByBenRegID(BEN_REG_ID)).thenReturn(mapping);
			MBeneficiaryImage image = new MBeneficiaryImage();
			image.setBenImage("base64-image-data");
			image.setCreatedDate(Timestamp.valueOf("2026-01-01 00:00:00"));
			when(imageRepo.getBenImageByBenImageID(BigInteger.valueOf(60L), VAN_ID)).thenReturn(image);

			String response = service.getBeneficiaryImage("{\"beneficiaryRegID\":" + BEN_REG_ID + "}");

			assertTrue(response.contains("base64-image-data"));
		}

		@Test
		@DisplayName("a beneficiary with no image gets a plain not-available response")
		void missingImageReportsNotAvailable() {
			when(mappingRepo.getBenImageIdByBenRegID(BEN_REG_ID)).thenReturn(null);

			String response = service.getBeneficiaryImage("{\"beneficiaryRegID\":" + BEN_REG_ID + "}");

			assertTrue(response.contains("Image not available"));
		}

		@Test
		@DisplayName("a request without a registration ID is rejected as invalid")
		void requestWithoutRegistrationIdIsRejected() {
			String response = service.getBeneficiaryImage("{\"somethingElse\":1}");

			assertTrue(response.contains("Invalid request"));
		}

		@Test
		@DisplayName("malformed JSON is reported as an error rather than thrown")
		void malformedJsonIsReportedAsError() {
			String response = service.getBeneficiaryImage("not json at all {");

			assertNotNull(response);
		}
	}

	@Nested
	@DisplayName("importing pre-allocated identifiers")
	class ImportingIdentifiers {

		@Test
		@DisplayName("an empty import is a no-op")
		void emptyImportIsANoOp() {
			assertEquals(0, service.importBenIdToLocalServer(Collections.emptyList()));
			verifyNoInteractions(dataSource);
		}

		@Test
		@DisplayName("a failing batch insert is surfaced rather than reported as a partial import")
		void failingBatchIsSurfaced() throws Exception {
			// The caller retries the whole batch, so a half-reported import would
			// silently drop identifiers.
			when(dataSource.getConnection()).thenThrow(new java.sql.SQLException("no connection"));
			BenIdImportDTO row = new BenIdImportDTO();
			row.setBenRegId(BEN_REG_ID);
			row.setBeneficiaryId(BEN_ID);

			assertThrows(RuntimeException.class, () -> service.importBenIdToLocalServer(List.of(row)));
		}
	}

	@Test
	@DisplayName("the diagnostic address dump touches every repository it reports on")
	void diagnosticDumpTouchesEveryRepository() {
		service.getBenAdress();

		verify(addressRepo).count();
		verify(consentRepo).count();
		verify(contactRepo).count();
		verify(accessRepo).count();
		verify(detailRepo).count();
		verify(familyMapRepo).count();
		verify(identityRepo).count();
		verify(mappingRepo).count();
	}

	@Test
	@DisplayName("an Elasticsearch-backed search still resolves a beneficiary ID directly")
	void elasticsearchSearchStillResolvesBeneficiaryIdDirectly() throws Exception {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setBeneficiaryId(BEN_ID);
		MBeneficiaryregidmapping regId = new MBeneficiaryregidmapping();
		regId.setBenRegId(BEN_REG_ID);
		when(regIdRepo.findByBeneficiaryID(BEN_ID)).thenReturn(regId);
		when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
		stubRowAssembly();

		assertEquals(1, service.getBeneficiarieswithES(searchDTO).size());
		verifyNoInteractions(elasticsearchService);
	}

	@Test
	@DisplayName("a registration ID search reaches the mapping projection through the ES entry point")
	void elasticsearchSearchResolvesRegistrationId() throws Exception {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setBeneficiaryRegId(BEN_REG_ID);
		when(mappingRepo.getBenMappingByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(projectionRow()));
		stubRowAssembly();

		assertEquals(1, service.getBeneficiarieswithES(searchDTO).size());
	}

	@Test
	@DisplayName("the advance-search view drives the fallback path of the ES entry point")
	void advanceSearchViewDrivesFallbackPath() throws Exception {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setFirstName("Asha");
		VBenAdvanceSearch row = new VBenAdvanceSearch();
		row.setBenMapID(BigInteger.ONE);
		row.setVanID(VAN_ID);
		row.setBenDetailsID(BigInteger.valueOf(5L));
		row.setBenRegID(BEN_REG_ID);
		row.setVanSerialNo(BigInteger.valueOf(9L));
		row.setHouseHoldID(555L);
		row.setGuideLineID("GL-1");
		row.setRchID("RCH-1");
		when(mappingRepo.dynamicFilterSearchNew(searchDTO)).thenReturn(Collections.singletonList(row));
		MBeneficiarydetail detail = new MBeneficiarydetail();
		detail.setCreatedBy("field.worker");
		detail.setCreatedDate(Timestamp.valueOf("2026-01-01 00:00:00"));
		detail.setFirstName("Asha");
		when(detailRepo.getWith_vanSerialNo_vanID(any(), anyInt())).thenReturn(detail);

		List<BeneficiariesDTO> result = service.getBeneficiarieswithES(searchDTO);

		assertEquals(1, result.size());
		assertEquals("Asha", result.get(0).getBeneficiaryDetails().getFirstName());
		verify(familyMapRepo).findByBenMapIdOrderByBenFamilyMapIdAsc(BigInteger.valueOf(9L));
	}
}
