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
package com.iemr.common.identity.service.rmnch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.iemr.common.identity.data.rmnch.NcdTbHrpData;
import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.data.rmnch.RMNCHBornBirthDetails;
import com.iemr.common.identity.data.rmnch.RMNCHCBACdetails;
import com.iemr.common.identity.data.rmnch.RMNCHHouseHoldDetails;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryAccount;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryImage;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryaddress;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarycontact;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarydetail;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarymapping;
import com.iemr.common.identity.repo.rmnch.RMNCHBenAccountRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenAddressRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenContactRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenImageRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBeneficiaryDetailsRmnchRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBornBirthDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHCBACDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHHouseHoldDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHMBenMappingRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHMBenRegIdMapRepo;
import com.iemr.common.identity.utils.exception.IEMRException;

/**
 * Tests for the RMNCH field-app synchronisation service.
 *
 * <p>
 * Field workers sync offline-collected data in bulk, and the app addresses
 * beneficiaries by the ID it was given at registration rather than by the
 * platform's registration ID. Every sync therefore has to translate IDs, decide
 * insert-versus-update per record by looking for an existing row, and keep going
 * when one beneficiary in a batch is unusable. Those three concerns are what
 * these tests pin down.
 *
 * <p>
 * The outbound HTTP calls to the teleconsultation and FHIR services are made
 * through a locally constructed client, so tests exercise the paths that do not
 * depend on those services being up, plus the failure handling for the ones
 * that do.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RmnchDataSyncServiceImplTest {

	@Mock
	private RMNCHBeneficiaryDetailsRmnchRepo benDetailsRmnchRepo;
	@Mock
	private RMNCHBornBirthDetailsRepo bornBirthRepo;
	@Mock
	private RMNCHCBACDetailsRepo cbacRepo;
	@Mock
	private RMNCHHouseHoldDetailsRepo houseHoldRepo;
	@Mock
	private RMNCHBenAddressRepo addressRepo;
	@Mock
	private RMNCHMBenMappingRepo mappingRepo;
	@Mock
	private RMNCHBenDetailsRepo detailsRepo;
	@Mock
	private RMNCHBenAccountRepo accountRepo;
	@Mock
	private RMNCHBenImageRepo imageRepo;
	@Mock
	private RMNCHBenContactRepo contactRepo;
	@Mock
	private RMNCHMBenRegIdMapRepo regIdMapRepo;

	@InjectMocks
	private RmnchDataSyncServiceImpl service;

	private static final BigInteger BEN_ID = BigInteger.valueOf(4001L);
	private static final BigInteger BEN_REG_ID = BigInteger.valueOf(100200300L);
	private static final Integer VAN_ID = 7;

	@BeforeEach
	void configurePageSize() {
		ReflectionTestUtils.setField(service, "door_to_door_page_size", "2");
	}

	@Nested
	@DisplayName("bulk sync from the field app")
	class BulkSync {

		@Test
		@DisplayName("a beneficiary-details record is translated to its registration ID and saved")
		void beneficiaryDetailsAreTranslatedAndSaved() throws Exception {
			when(regIdMapRepo.getRegID(BEN_ID)).thenReturn(BEN_REG_ID);
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(detailsRepo.getByBenRegID(any())).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBeneficiaryDetailsRmnch> supplied = invocation.getArgument(0);
				supplied.forEach(record -> record.setId(11L));
				return new ArrayList<>(supplied);
			});

			String response = service.syncDataToAmrit(
					"{\"beneficiaryDetails\":[{\"benficieryid\":4001,\"firstName\":\"Asha\"}]}");

			ArgumentCaptor<List<RMNCHBeneficiaryDetailsRmnch>> captor = captor();
			verify(benDetailsRmnchRepo).saveAll(captor.capture());
			assertEquals(BEN_REG_ID, captor.getValue().get(0).getBenRegId());
			assertTrue(response.contains("11"));
		}

		@Test
		@DisplayName("an existing RMNCH row is updated in place rather than duplicated")
		void existingRowIsUpdatedInPlace() throws Exception {
			when(regIdMapRepo.getRegID(BEN_ID)).thenReturn(BEN_REG_ID);
			RMNCHBeneficiaryDetailsRmnch existing = new RMNCHBeneficiaryDetailsRmnch();
			existing.setBeneficiaryDetails_RmnchId(BigInteger.valueOf(55L));
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(existing));
			when(detailsRepo.getByBenRegID(any())).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBeneficiaryDetailsRmnch> supplied = invocation.getArgument(0);
				return new ArrayList<>(supplied);
			});

			service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001,\"firstName\":\"Asha\"}]}");

			ArgumentCaptor<List<RMNCHBeneficiaryDetailsRmnch>> captor = captor();
			verify(benDetailsRmnchRepo).saveAll(captor.capture());
			assertEquals(BigInteger.valueOf(55L), captor.getValue().get(0).getBeneficiaryDetails_RmnchId());
		}

		@Test
		@DisplayName("names from the sync are written back onto the platform's own detail row")
		void namesAreWrittenBackToThePlatformRow() throws Exception {
			when(regIdMapRepo.getRegID(BEN_ID)).thenReturn(BEN_REG_ID);
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			RMNCHMBeneficiarydetail platformRow = new RMNCHMBeneficiarydetail();
			when(detailsRepo.getByBenRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(platformRow));
			when(benDetailsRmnchRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBeneficiaryDetailsRmnch> supplied = invocation.getArgument(0);
				return new ArrayList<>(supplied);
			});

			service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001,\"firstName\":\"Asha\","
					+ "\"lastName\":\"Devi\",\"genderId\":2,\"gender\":\"Female\"}]}");

			verify(detailsRepo).saveAll(any());
			assertEquals("Asha", platformRow.getFirstName());
			assertEquals("Devi", platformRow.getLastName());
			assertEquals("Female", platformRow.getGender());
		}

		@Test
		@DisplayName("related beneficiary IDs are flattened to the comma-separated column the schema stores")
		void relatedBeneficiaryIdsAreFlattened() throws Exception {
			// The separator loop never advances its pointer, so its
			// last-element check never fires and the stored value carries a
			// trailing comma. Reading it back still yields the right IDs because
			// String.split discards the trailing empty field.
			when(regIdMapRepo.getRegID(BEN_ID)).thenReturn(BEN_REG_ID);
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(detailsRepo.getByBenRegID(any())).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBeneficiaryDetailsRmnch> supplied = invocation.getArgument(0);
				return new ArrayList<>(supplied);
			});

			service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001,"
					+ "\"relatedBeneficiaryIds\":[11,22,33]}]}");

			ArgumentCaptor<List<RMNCHBeneficiaryDetailsRmnch>> captor = captor();
			verify(benDetailsRmnchRepo).saveAll(captor.capture());
			assertEquals("11,22,33,", captor.getValue().get(0).getRelatedBeneficiaryIdsDB());
		}

		@Test
		@DisplayName("birth details in the same payload are synced alongside the beneficiary")
		void birthDetailsAreSyncedAlongside() throws Exception {
			stubBeneficiaryDetailsSync();
			when(bornBirthRepo.getByRegID(BEN_REG_ID)).thenReturn(null);
			when(bornBirthRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBornBirthDetails> supplied = invocation.getArgument(0);
				supplied.forEach(record -> record.setId(22L));
				return new ArrayList<>(supplied);
			});

			String response = service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001}],"
					+ "\"bornBirthDeatils\":[{\"benficieryid\":4001}]}");

			verify(bornBirthRepo).saveAll(any());
			assertTrue(response.contains("22"));
		}

		@Test
		@DisplayName("an existing birth-details row is updated in place")
		void existingBirthDetailsRowIsUpdatedInPlace() throws Exception {
			stubBeneficiaryDetailsSync();
			RMNCHBornBirthDetails existing = new RMNCHBornBirthDetails();
			existing.setBornBirthDeatilsId(66L);
			when(bornBirthRepo.getByRegID(BEN_REG_ID)).thenReturn(existing);
			when(bornBirthRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBornBirthDetails> supplied = invocation.getArgument(0);
				return new ArrayList<>(supplied);
			});

			service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001}],"
					+ "\"bornBirthDeatils\":[{\"benficieryid\":4001}]}");

			ArgumentCaptor<List<RMNCHBornBirthDetails>> captor = captor();
			verify(bornBirthRepo).saveAll(captor.capture());
			assertEquals(66L, captor.getValue().get(0).getBornBirthDeatilsId());
		}

		@Test
		@DisplayName("a synced CBAC screening starts with its diagnosis outcomes unchecked")
		void syncedCbacScreeningStartsUnchecked() throws Exception {
			stubBeneficiaryDetailsSync();
			when(cbacRepo.getByRegID(BEN_REG_ID)).thenReturn(null);
			when(cbacRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHCBACdetails> supplied = invocation.getArgument(0);
				supplied.forEach(record -> record.setId(33L));
				return new ArrayList<>(supplied);
			});

			service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001}],"
					+ "\"cBACDetails\":[{\"benficieryid\":4001}]}");

			ArgumentCaptor<List<RMNCHCBACdetails>> captor = captor();
			verify(cbacRepo).saveAll(captor.capture());
			RMNCHCBACdetails saved = captor.getValue().get(0);
			assertEquals("Not checked", saved.getConfirmed_hrp());
			assertEquals("Not checked", saved.getConfirmed_ncd());
			assertEquals("Not checked", saved.getConfirmed_tb());
			assertEquals("Not checked", saved.getConfirmed_ncd_diseases());
			assertEquals("pending", saved.getDiagnosis_status());
		}

		@Test
		@DisplayName("household details are keyed on the household ID, not the beneficiary")
		void householdDetailsAreKeyedOnHouseholdId() throws Exception {
			stubBeneficiaryDetailsSync();
			RMNCHHouseHoldDetails existing = new RMNCHHouseHoldDetails();
			existing.setHouseHoldDetailsId(77L);
			when(houseHoldRepo.getByHouseHoldID(anyLong())).thenReturn(Collections.singletonList(existing));
			when(houseHoldRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHHouseHoldDetails> supplied = invocation.getArgument(0);
				supplied.forEach(record -> record.setId(44L));
				return new ArrayList<>(supplied);
			});

			String response = service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001}],"
					+ "\"houseHoldDetails\":[{\"houseoldId\":555}]}");

			ArgumentCaptor<List<RMNCHHouseHoldDetails>> captor = captor();
			verify(houseHoldRepo).saveAll(captor.capture());
			assertEquals(77L, captor.getValue().get(0).getHouseHoldDetailsId());
			assertTrue(response.contains("44"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "", "{}", "{\"beneficiaryDetails\":[]}" })
		@DisplayName("a payload with no beneficiary is rejected rather than silently syncing nothing")
		void payloadWithNoBeneficiaryIsRejected(String payload) {
			assertThrows(Exception.class, () -> service.syncDataToAmrit(payload));
			verify(benDetailsRmnchRepo, never()).saveAll(any());
		}

		@Test
		@DisplayName("a null payload is rejected")
		void nullPayloadIsRejected() {
			assertThrows(Exception.class, () -> service.syncDataToAmrit(null));
		}

		@Test
		@DisplayName("a persistence failure aborts the whole sync so the app retries the batch")
		void persistenceFailureAbortsTheSync() {
			when(regIdMapRepo.getRegID(any())).thenReturn(BEN_REG_ID);
			when(benDetailsRmnchRepo.getByRegID(any())).thenReturn(Collections.emptyList());
			when(detailsRepo.getByBenRegID(any())).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.saveAll(any())).thenThrow(new IllegalStateException("deadlock"));

			assertThrows(Exception.class,
					() -> service.syncDataToAmrit("{\"beneficiaryDetails\":[{\"benficieryid\":4001}]}"));
		}

		private void stubBeneficiaryDetailsSync() {
			when(regIdMapRepo.getRegID(BEN_ID)).thenReturn(BEN_REG_ID);
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(detailsRepo.getByBenRegID(any())).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.saveAll(any())).thenAnswer(invocation -> {
				List<RMNCHBeneficiaryDetailsRmnch> supplied = invocation.getArgument(0);
				return new ArrayList<>(supplied);
			});
		}
	}

	@Nested
	@DisplayName("saving RMNCH details after a registration")
	class SavingAfterRegistration {

		/**
		 * The minimum request the method can actually process: every integer
		 * field it reads with a {@code null} fallback has to be present. See
		 * {@link #requestOmittingAnIntegerFieldIsRejected()}.
		 */
		private String requestWith(String extraFields) {
			String base = "\"vanID\":7,\"parkingPlaceID\":3,\"providerServiceMapID\":11,"
					+ "\"genderID\":2,\"maritalStatusID\":3";
			return "{" + base + (extraFields.isEmpty() ? "" : "," + extraFields) + "}";
		}

		@Test
		@DisplayName("a request that omits an optional integer field saves nothing")
		void requestOmittingAnIntegerFieldIsRejected() {
			// getInt(obj, key, null) unboxes its fallback, so an absent vanID -
			// which the registration UI does not always send - throws before the
			// row is written. The failure is swallowed into the return value, so
			// the caller sees registration succeed with no RMNCH row behind it.
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			String response = service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					"{\"createdBy\":\"field.worker\"}");

			assertTrue(response.startsWith("Error save beneficiary in rmnch"));
			verify(benDetailsRmnchRepo, never()).save(any());
		}

		@Test
		@DisplayName("a beneficiary with no RMNCH row gets one stamped with the creating user")
		void newRowIsStampedWithTheCreatingUser() {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			String response = service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					requestWith("\"createdBy\":\"field.worker\",\"firstName\":\"Asha\""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals("field.worker", captor.getValue().getCreatedBy());
			assertNotNull(captor.getValue().getCreatedDate());
			assertNull(captor.getValue().getUpdatedBy());
			assertEquals(BEN_ID, captor.getValue().getBenficieryid());
			assertEquals(BEN_REG_ID, captor.getValue().getBenRegId());
			assertTrue(response.contains("4001"));
		}

		@Test
		@DisplayName("an existing row records the modifying user instead of the creating one")
		void existingRowRecordsTheModifyingUser() {
			RMNCHBeneficiaryDetailsRmnch existing = new RMNCHBeneficiaryDetailsRmnch();
			existing.setCreatedBy("original.worker");
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(existing));
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					requestWith("\"createdBy\":\"second.worker\""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals("original.worker", captor.getValue().getCreatedBy());
			assertEquals("second.worker", captor.getValue().getUpdatedBy());
			assertNotNull(captor.getValue().getUpdatedDate());
		}

		@Test
		@DisplayName("an omitted creating user is recorded as the system")
		void omittedCreatingUserIsRecordedAsSystem() {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L, requestWith(""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals("system", captor.getValue().getCreatedBy());
		}

		@Test
		@DisplayName("the reproductive status falls back to the marital status when not sent")
		void reproductiveStatusFallsBackToMaritalStatus() {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L, requestWith(""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals(3, captor.getValue().getReproductiveStatusId());
			assertEquals(3, captor.getValue().getMaritalstatusId());
		}

		@Test
		@DisplayName("an explicit reproductive status wins over the marital status")
		void explicitReproductiveStatusWins() {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					requestWith("\"reproductiveStatusId\":9,\"reproductiveStatus\":\"Pregnant\""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals(9, captor.getValue().getReproductiveStatusId());
			assertEquals("Pregnant", captor.getValue().getReproductiveStatus());
		}

		@ParameterizedTest
		@ValueSource(strings = { "1996-01-15 00:00:00", "1996-01-15T00:00:00Z", "1996-01-15T00:00:00" })
		@DisplayName("a date of birth is accepted in each form the registration UI sends it")
		void dateOfBirthIsAcceptedInEachForm(String suppliedDob) {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					requestWith("\"dOB\":\"" + suppliedDob + "\""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertEquals(Timestamp.valueOf("1996-01-15 00:00:00"), captor.getValue().getDob());
		}

		@ParameterizedTest
		@ValueSource(strings = { "15/01/1996", "", "   " })
		@DisplayName("an unusable date of birth is skipped rather than failing the registration")
		void unusableDateOfBirthIsSkipped(String suppliedDob) {
			// The beneficiary is already registered by this point, so losing the
			// DOB is preferable to failing the call.
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(benDetailsRmnchRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

			service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L,
					requestWith("\"dOB\":\"" + suppliedDob + "\""));

			ArgumentCaptor<RMNCHBeneficiaryDetailsRmnch> captor = ArgumentCaptor
					.forClass(RMNCHBeneficiaryDetailsRmnch.class);
			verify(benDetailsRmnchRepo).save(captor.capture());
			assertNull(captor.getValue().getDob());
		}

		@Test
		@DisplayName("a failure is reported in the response rather than thrown at the registration flow")
		void failureIsReportedInTheResponse() {
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenThrow(new IllegalStateException("table locked"));

			String response = service.saveBeneficiaryDetailsAfterRegistration(4001L, 100200300L, "{}");

			assertTrue(response.startsWith("Error save beneficiary in rmnch"));
		}
	}

	@Nested
	@DisplayName("door-to-door fetch by village")
	class FetchByVillage {

		@Test
		@DisplayName("a village page is fetched with the configured page size")
		void villagePageIsFetchedWithConfiguredPageSize() throws Exception {
			when(addressRepo.getBenData(eq(401), any())).thenReturn(pageOf(address()));
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(null);

			String response = service.getBenData("{\"villageID\":401,\"pageNo\":0}", "Bearer token");

			assertNotNull(response);
			ArgumentCaptor<PageRequest> captor = ArgumentCaptor.forClass(PageRequest.class);
			verify(addressRepo).getBenData(eq(401), captor.capture());
			assertEquals(2, captor.getValue().getPageSize());
			assertEquals(0, captor.getValue().getPageNumber());
		}

		@Test
		@DisplayName("a date range narrows the query to the range-filtered variant")
		void dateRangeUsesTheFilteredQuery() throws Exception {
			when(addressRepo.getBenDataFilteredWithDateRange(eq(401), any(), any(), any()))
					.thenReturn(pageOf(address()));

			service.getBenData("{\"villageID\":401,\"pageNo\":0,\"fromDate\":\"2026-01-01T00:00:00Z\","
					+ "\"toDate\":\"2026-02-01T00:00:00Z\"}", "Bearer token");

			verify(addressRepo).getBenDataFilteredWithDateRange(eq(401), any(), any(), any());
			verify(addressRepo, never()).getBenData(anyInt(), any());
		}

		@Test
		@DisplayName("a request without a village is rejected")
		void requestWithoutVillageIsRejected() {
			Exception thrown = assertThrows(Exception.class, () -> service.getBenData("{\"pageNo\":0}", "token"));

			assertTrue(thrown.getMessage().contains("village"));
		}

		@Test
		@DisplayName("a request without a page number is rejected")
		void requestWithoutPageNumberIsRejected() {
			Exception thrown = assertThrows(Exception.class,
					() -> service.getBenData("{\"villageID\":401}", "token"));

			assertTrue(thrown.getMessage().contains("page no"));
		}

		@Test
		@DisplayName("an empty village page yields no payload at all")
		void emptyVillagePageYieldsNoPayload() throws Exception {
			when(addressRepo.getBenData(eq(401), any())).thenReturn(new PageImpl<>(Collections.emptyList()));

			assertNull(service.getBenData("{\"villageID\":401,\"pageNo\":0}", "token"));
		}
	}

	@Nested
	@DisplayName("door-to-door fetch by ASHA worker")
	class FetchByAsha {

		@Test
		@DisplayName("the ASHA's ID is resolved to the username the address rows are stamped with")
		void ashaIdIsResolvedToUsername() throws Exception {
			when(addressRepo.getUserNameForAsha(9)).thenReturn("asha.worker");
			when(addressRepo.getBenDataByAsha(eq("asha.worker"), any())).thenReturn(pageOf(address()));

			assertNotNull(service.getBenDataByAsha("{\"AshaId\":9,\"pageNo\":0}", "token"));

			verify(addressRepo).getBenDataByAsha(eq("asha.worker"), any());
		}

		@Test
		@DisplayName("a date range narrows the ASHA query to the range-filtered variant")
		void dateRangeUsesTheFilteredQuery() throws Exception {
			when(addressRepo.getUserNameForAsha(9)).thenReturn("asha.worker");
			when(addressRepo.getBenDataByAshaFilteredWithDateRange(eq("asha.worker"), any(), any(), any()))
					.thenReturn(pageOf(address()));

			service.getBenDataByAsha("{\"AshaId\":9,\"pageNo\":0,\"fromDate\":\"2026-01-01T00:00:00Z\","
					+ "\"toDate\":\"2026-02-01T00:00:00Z\"}", "token");

			verify(addressRepo).getBenDataByAshaFilteredWithDateRange(eq("asha.worker"), any(), any(), any());
		}

		@ParameterizedTest
		@CsvSource(nullValues = "null", value = { "null", "''" })
		@DisplayName("an unrecognised ASHA ID is reported as a configuration problem")
		void unrecognisedAshaIdIsReported(String resolvedUsername) {
			when(addressRepo.getUserNameForAsha(9)).thenReturn(resolvedUsername);

			Exception thrown = assertThrows(Exception.class,
					() -> service.getBenDataByAsha("{\"AshaId\":9,\"pageNo\":0}", "token"));

			assertTrue(thrown.getMessage().contains("Asha details not found"));
		}

		@Test
		@DisplayName("a request without an ASHA ID is rejected")
		void requestWithoutAshaIdIsRejected() {
			assertThrows(Exception.class, () -> service.getBenDataByAsha("{\"pageNo\":0}", "token"));
		}

		@Test
		@DisplayName("a request without a page number is rejected")
		void requestWithoutPageNumberIsRejected() {
			when(addressRepo.getUserNameForAsha(9)).thenReturn("asha.worker");

			Exception thrown = assertThrows(Exception.class,
					() -> service.getBenDataByAsha("{\"AshaId\":9}", "token"));

			assertTrue(thrown.getMessage().contains("page no"));
		}
	}

	@Nested
	@DisplayName("assembling the door-to-door payload")
	class AssemblingPayload {

		@BeforeEach
		void stubAddressPage() {
			when(addressRepo.getBenData(eq(401), any())).thenReturn(pageOf(address()));
		}

		private RMNCHMBeneficiarymapping mapping() {
			RMNCHMBeneficiarymapping mapping = new RMNCHMBeneficiarymapping();
			mapping.setBenRegId(BEN_REG_ID);
			mapping.setVanID(VAN_ID);
			mapping.setBenDetailsId(BigInteger.valueOf(5L));
			mapping.setBenAccountID(BigInteger.valueOf(7L));
			mapping.setBenImageId(BigInteger.valueOf(6L));
			mapping.setBenAddressId(BigInteger.valueOf(2L));
			mapping.setBenContactsId(BigInteger.valueOf(4L));
			return mapping;
		}

		private String fetch() throws Exception {
			return service.getBenData("{\"villageID\":401,\"pageNo\":0}", "Bearer token");
		}

		@Test
		@DisplayName("an address with no mapping row contributes nothing to the payload")
		void addressWithNoMappingContributesNothing() throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(null);

			String response = fetch();

			assertTrue(response.contains("\"data\":[]"));
			assertTrue(response.contains("\"totalPage\":1"));
			assertTrue(response.contains("\"pageSize\":2"));
		}

		@Test
		@DisplayName("the platform's bank, address and contact rows are folded into the RMNCH record")
		void platformRowsAreFoldedIn() throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			RMNCHMBeneficiarydetail detail = new RMNCHMBeneficiarydetail();
			detail.setFirstName("Asha");
			detail.setLastName("Devi");
			detail.setMotherName("Sita");
			detail.setCreatedBy("field.worker");
			detail.setDob(Timestamp.valueOf("1996-01-15 00:00:00"));
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenReturn(detail);
			RMNCHMBeneficiaryAccount account = new RMNCHMBeneficiaryAccount();
			account.setNameOfBank("SBI");
			account.setBranchName("Yelahanka");
			account.setIfscCode("SBIN0001");
			account.setBankAccount("1234567890");
			when(accountRepo.getByIdAndVanID(any(), anyInt())).thenReturn(account);
			RMNCHMBeneficiaryaddress benAddress = new RMNCHMBeneficiaryaddress();
			benAddress.setPermState("Karnataka");
			benAddress.setPermAddrLine1("1 Main Rd");
			benAddress.setCreatedBy("field.worker");
			when(addressRepo.getByIdAndVanID(any(), anyInt())).thenReturn(benAddress);
			RMNCHMBeneficiarycontact contact = new RMNCHMBeneficiarycontact();
			contact.setPreferredPhoneNum("9000000000");
			when(contactRepo.getByIdAndVanID(any(), anyInt())).thenReturn(contact);
			when(imageRepo.getByIdAndVanID(anyLong(), anyInt())).thenReturn(new RMNCHMBeneficiaryImage());
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());
			when(mappingRepo.getUserIDByUserName("field.worker")).thenReturn(31);

			String response = fetch();

			assertTrue(response.contains("SBI"));
			assertTrue(response.contains("Karnataka"));
			assertTrue(response.contains("9000000000"));
			assertTrue(response.contains("Sita"));
			assertTrue(response.contains("\"ashaId\":31"));
		}

		@Test
		@DisplayName("the stored comma-separated related IDs are expanded back into a list")
		void relatedIdsAreExpandedBackIntoAList() throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			stubEmptyPlatformRows();
			RMNCHBeneficiaryDetailsRmnch rmnch = new RMNCHBeneficiaryDetailsRmnch();
			rmnch.setRelatedBeneficiaryIdsDB("11,22,33");
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.singletonList(rmnch));

			String response = fetch();

			assertTrue(response.contains("11"));
			assertTrue(response.contains("33"));
		}

		@ParameterizedTest
		@CsvSource({ "30, Years", "1, Year" })
		@DisplayName("an age in whole years is rendered with the matching unit")
		void ageInYearsIsRenderedWithMatchingUnit(int yearsAgo, String expectedUnit) throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			RMNCHMBeneficiarydetail detail = new RMNCHMBeneficiarydetail();
			detail.setDob(Timestamp.valueOf(
					java.time.LocalDate.now().minusYears(yearsAgo).minusDays(1).atStartOfDay()));
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenReturn(detail);
			stubEmptyPlatformRowsExceptDetail();
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			String response = fetch();

			assertTrue(response.contains(expectedUnit), response);
		}

		@Test
		@DisplayName("an infant's age is rendered in months")
		void infantAgeIsRenderedInMonths() throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			RMNCHMBeneficiarydetail detail = new RMNCHMBeneficiarydetail();
			detail.setDob(Timestamp.valueOf(java.time.LocalDate.now().minusMonths(3).atStartOfDay()));
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenReturn(detail);
			stubEmptyPlatformRowsExceptDetail();
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			String response = fetch();

			assertTrue(response.contains("Months"), response);
		}

		@Test
		@DisplayName("a newborn's age is rendered in days")
		void newbornAgeIsRenderedInDays() throws Exception {
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			RMNCHMBeneficiarydetail detail = new RMNCHMBeneficiarydetail();
			detail.setDob(Timestamp.valueOf(java.time.LocalDate.now().minusDays(5).atStartOfDay()));
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenReturn(detail);
			stubEmptyPlatformRowsExceptDetail();
			when(benDetailsRmnchRepo.getByRegID(BEN_REG_ID)).thenReturn(Collections.emptyList());

			String response = fetch();

			assertTrue(response.contains("Days"), response);
		}

		@Test
		@DisplayName("one unusable beneficiary does not lose the rest of the page")
		void oneUnusableBeneficiaryDoesNotLoseThePage() throws Exception {
			when(addressRepo.getBenData(eq(401), any())).thenReturn(pageOf(address(), address()));
			when(mappingRepo.getByAddressIDAndVanID(any(), anyInt())).thenReturn(mapping());
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenThrow(new IllegalStateException("row corrupt"));

			String response = fetch();

			assertTrue(response.contains("\"data\":[]"));
		}

		private void stubEmptyPlatformRows() {
			when(detailsRepo.getByIdAndVanID(any(), anyInt())).thenReturn(new RMNCHMBeneficiarydetail());
			stubEmptyPlatformRowsExceptDetail();
		}

		private void stubEmptyPlatformRowsExceptDetail() {
			when(accountRepo.getByIdAndVanID(any(), anyInt())).thenReturn(new RMNCHMBeneficiaryAccount());
			when(imageRepo.getByIdAndVanID(anyLong(), anyInt())).thenReturn(new RMNCHMBeneficiaryImage());
			when(addressRepo.getByIdAndVanID(any(), anyInt())).thenReturn(new RMNCHMBeneficiaryaddress());
			when(contactRepo.getByIdAndVanID(any(), anyInt())).thenReturn(new RMNCHMBeneficiarycontact());
		}
	}

	@Nested
	@DisplayName("suspected-condition status")
	class SuspectedConditionStatus {

		@Test
		@DisplayName("a beneficiary with no recorded visit has no suspected-condition status")
		void noVisitYieldsNoStatus() throws Exception {
			when(cbacRepo.getVisitDetailsbyRegID(100200300L)).thenReturn(Collections.emptyList());

			assertNull(service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail()));
		}

		@Test
		@DisplayName("no beneficiary means no status rather than a lookup")
		void noBeneficiaryMeansNoStatus() throws Exception {
			assertNull(service.getHRP_NCD_TB_SuspectedStatus(null, "token", maleDetail()));

			verify(cbacRepo, never()).getVisitDetailsbyRegID(any());
		}

		@ParameterizedTest
		@ValueSource(strings = { "General OPD", "General OPD (QC)", "NCD screening", "COVID-19 Screening" })
		@DisplayName("the common visit categories read the general diagnosis column")
		void commonVisitCategoriesReadTheGeneralColumn(String visitCategory) throws Exception {
			stubVisit(visitCategory);
			when(cbacRepo.getDiagnosisProvidedCommon(100200300L, 900L))
					.thenReturn(Collections.singletonList("Tuberculosis"));

			NcdTbHrpData result = service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail());

			assertEquals("Yes", result.getConfirmed_tb());
			assertEquals("No", result.getConfirmed_hrp());
		}

		@Test
		@DisplayName("a post-natal visit reads the PNC diagnosis column")
		void postNatalVisitReadsThePncColumn() throws Exception {
			stubVisit("PNC");
			when(cbacRepo.getDiagnosisProvidedPNC(100200300L, 900L))
					.thenReturn(Collections.singletonList("Hypertension"));

			NcdTbHrpData result = service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail());

			assertEquals("Yes", result.getConfirmed_ncd());
			assertEquals("Hypertension", result.getConfirmed_ncd_diseases());
		}

		@Test
		@DisplayName("an NCD-care visit reads the NCD-care diagnosis column")
		void ncdCareVisitReadsTheNcdCareColumn() throws Exception {
			stubVisit("NCD care");
			when(cbacRepo.getDiagnosisProvidedNCDCare(100200300L, 900L))
					.thenReturn(Collections.singletonList("Diabetes mellitus"));

			NcdTbHrpData result = service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail());

			assertEquals("Yes", result.getConfirmed_ncd());
			// The "No" is only written once a TB verdict has already been
			// recorded, so a single non-TB diagnosis leaves it unset rather than
			// asserting the beneficiary is TB-free.
			assertNull(result.getConfirmed_tb());
		}

		@Test
		@DisplayName("an antenatal visit only reports the high-risk-pregnancy status")
		void antenatalVisitOnlyReportsHrp() throws Exception {
			stubVisit("ANC");

			NcdTbHrpData result = service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail());

			assertEquals("No", result.getConfirmed_hrp());
			verify(cbacRepo, never()).getDiagnosisProvidedCommon(any(), any());
		}

		@Test
		@DisplayName("an unrecognised visit category yields an empty status")
		void unrecognisedVisitCategoryYieldsEmptyStatus() throws Exception {
			stubVisit("Dental");

			NcdTbHrpData result = service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail());

			assertNotNull(result);
			assertNull(result.getConfirmed_tb());
		}

		@Test
		@DisplayName("a failing visit lookup is reported as a service error")
		void failingVisitLookupIsReported() {
			when(cbacRepo.getVisitDetailsbyRegID(100200300L)).thenThrow(new IllegalStateException("view down"));

			assertThrows(IEMRException.class,
					() -> service.getHRP_NCD_TB_SuspectedStatus(100200300L, "token", maleDetail()));
		}

		private void stubVisit(String visitCategory) {
			when(cbacRepo.getVisitDetailsbyRegID(100200300L))
					.thenReturn(Collections.singletonList(new Object[] { BigInteger.valueOf(900L), visitCategory }));
		}
	}

	@Nested
	@DisplayName("diagnosis interpretation")
	class DiagnosisInterpretation {

		@Test
		@DisplayName("a beneficiary with no diagnosis is left pending")
		void noDiagnosisIsLeftPending() throws Exception {
			when(cbacRepo.getDiagnosisProvidedCommon(100200300L, 900L)).thenReturn(Collections.emptyList());

			NcdTbHrpData result = service.getConfirmedNCD_TB_Common(100200300L, 900L);

			assertEquals("Pending", result.getDiagnosis_status());
			assertNull(result.getConfirmed_tb());
		}

		@Test
		@DisplayName("a missing visit or beneficiary leaves the status pending without a lookup")
		void missingVisitLeavesStatusPending() throws Exception {
			assertEquals("Pending", service.getConfirmedNCD_TB_Common(null, 900L).getDiagnosis_status());
			assertEquals("Pending", service.getConfirmedNCD_TB_Common(100200300L, null).getDiagnosis_status());
			assertEquals("Pending", service.getConfirmedNCD_TB_PNC(null, null).getDiagnosis_status());
			assertEquals("Pending", service.getConfirmedNCD_TB_NCD_CARE(null, null).getDiagnosis_status());

			verify(cbacRepo, never()).getDiagnosisProvidedCommon(any(), any());
		}

		@ParameterizedTest
		@ValueSource(strings = { "Diabetes mellitus", "Hypertension", "Breast cancer", "Mental health disorder",
				"Oral cancer" })
		@DisplayName("each tracked non-communicable disease is recognised")
		void eachTrackedNcdIsRecognised(String diagnosis) throws Exception {
			when(cbacRepo.getDiagnosisProvidedCommon(100200300L, 900L))
					.thenReturn(Collections.singletonList(diagnosis));

			NcdTbHrpData result = service.getConfirmedNCD_TB_Common(100200300L, 900L);

			assertEquals("Yes", result.getConfirmed_ncd());
			assertEquals(diagnosis, result.getConfirmed_ncd_diseases());
			assertEquals("Yes", result.getDiagnosis_status());
		}

		@Test
		@DisplayName("multiple diagnoses in one visit are all listed")
		void multipleDiagnosesAreAllListed() throws Exception {
			when(cbacRepo.getDiagnosisProvidedCommon(100200300L, 900L))
					.thenReturn(Collections.singletonList("Hypertension||Diabetes mellitus||Tuberculosis"));

			NcdTbHrpData result = service.getConfirmedNCD_TB_Common(100200300L, 900L);

			assertEquals("Yes", result.getConfirmed_ncd());
			assertEquals("Yes", result.getConfirmed_tb());
			assertEquals("Hypertension,Diabetes mellitus", result.getConfirmed_ncd_diseases());
		}

		@Test
		@DisplayName("an untracked diagnosis is recorded as neither TB nor NCD")
		void untrackedDiagnosisIsRecordedAsNeither() throws Exception {
			when(cbacRepo.getDiagnosisProvidedCommon(100200300L, 900L))
					.thenReturn(Collections.singletonList("Common cold"));

			NcdTbHrpData result = service.getConfirmedNCD_TB_Common(100200300L, 900L);

			assertEquals("Yes", result.getDiagnosis_status());
			assertNull(result.getConfirmed_ncd_diseases());
		}

		@Test
		@DisplayName("the PNC and NCD-care columns are interpreted the same way as the general one")
		void pncAndNcdCareAreInterpretedTheSameWay() throws Exception {
			when(cbacRepo.getDiagnosisProvidedPNC(100200300L, 900L))
					.thenReturn(Collections.singletonList("Tuberculosis||Oral cancer"));
			when(cbacRepo.getDiagnosisProvidedNCDCare(100200300L, 900L))
					.thenReturn(Collections.singletonList("Tuberculosis||Oral cancer"));

			NcdTbHrpData pnc = service.getConfirmedNCD_TB_PNC(100200300L, 900L);
			NcdTbHrpData ncdCare = service.getConfirmedNCD_TB_NCD_CARE(100200300L, 900L);

			assertEquals("Yes", pnc.getConfirmed_tb());
			assertEquals("Oral cancer", pnc.getConfirmed_ncd_diseases());
			assertEquals("Yes", ncdCare.getConfirmed_tb());
			assertEquals("Oral cancer", ncdCare.getConfirmed_ncd_diseases());
		}

		@Test
		@DisplayName("a failing diagnosis lookup is reported as a service error on every column")
		void failingDiagnosisLookupIsReported() {
			when(cbacRepo.getDiagnosisProvidedCommon(any(), any())).thenThrow(new IllegalStateException("view down"));
			when(cbacRepo.getDiagnosisProvidedPNC(any(), any())).thenThrow(new IllegalStateException("view down"));
			when(cbacRepo.getDiagnosisProvidedNCDCare(any(), any()))
					.thenThrow(new IllegalStateException("view down"));

			assertThrows(IEMRException.class, () -> service.getConfirmedNCD_TB_Common(100200300L, 900L));
			assertThrows(IEMRException.class, () -> service.getConfirmedNCD_TB_PNC(100200300L, 900L));
			assertThrows(IEMRException.class, () -> service.getConfirmedNCD_TB_NCD_CARE(100200300L, 900L));
		}
	}

	@Nested
	@DisplayName("high-risk-pregnancy status")
	class HighRiskPregnancyStatus {

		@Test
		@DisplayName("a male beneficiary is never high-risk and is not looked up remotely")
		void maleBeneficiaryIsNeverHighRisk() throws Exception {
			assertEquals("No", service.getConfirmedHRP(100200300L, 900L, "token", maleDetail()));
		}

		@Test
		@DisplayName("a beneficiary with no recorded gender is not looked up remotely")
		void unknownGenderIsNotLookedUpRemotely() throws Exception {
			assertEquals("No", service.getConfirmedHRP(100200300L, 900L, "token", new RMNCHMBeneficiarydetail()));
			assertEquals("No", service.getConfirmedHRP(100200300L, 900L, "token", null));
		}

		@Test
		@DisplayName("an unreachable teleconsultation service is reported as a service error")
		void unreachableTeleconsultationServiceIsReported() {
			// The status drives clinical follow-up, so an unknown answer must not
			// be reported as "not high risk".
			RMNCHMBeneficiarydetail female = new RMNCHMBeneficiarydetail();
			female.setGender("Female");

			assertThrows(IEMRException.class, () -> service.getConfirmedHRP(100200300L, 900L, "token", female));
		}
	}

	@Test
	@DisplayName("an unreachable FHIR service leaves the ABHA list unknown rather than empty")
	void unreachableFhirServiceLeavesAbhaListUnknown() {
		assertNull(service.fetchHealthIdByBenRegID(100200300L, "token"));
	}

	private RMNCHMBeneficiarydetail maleDetail() {
		RMNCHMBeneficiarydetail detail = new RMNCHMBeneficiarydetail();
		detail.setGender("Male");
		return detail;
	}

	private RMNCHMBeneficiaryaddress address() {
		RMNCHMBeneficiaryaddress address = new RMNCHMBeneficiaryaddress();
		address.setId(BigInteger.valueOf(2L));
		address.setVanID(VAN_ID);
		return address;
	}

	private Page<RMNCHMBeneficiaryaddress> pageOf(RMNCHMBeneficiaryaddress... addresses) {
		return new PageImpl<>(Arrays.asList(addresses));
	}

	@SuppressWarnings("unchecked")
	private <T> ArgumentCaptor<List<T>> captor() {
		return ArgumentCaptor.forClass(List.class);
	}
}
