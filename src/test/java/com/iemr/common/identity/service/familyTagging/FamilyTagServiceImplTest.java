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
package com.iemr.common.identity.service.familyTagging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.iemr.common.identity.data.familyTagging.BenFamilyMapping;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.exception.IEMRException;
import com.iemr.common.identity.repo.BenDetailRepo;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.familyTag.FamilyTagRepo;

/**
 * Tests for family tagging.
 *
 * <p>
 * A family is a row in {@code i_benfamilymapping} carrying a member count and a
 * head-of-family name, plus a {@code familyId} stamped onto each member's
 * beneficiary-detail row. Nothing in the schema keeps those two in step, so the
 * service does it by hand: tagging increments the count, untagging decrements
 * it, and either can clear the head name. These tests pin that bookkeeping, and
 * the fact that every failure comes back as an {@link IEMRException} rather
 * than a half-applied change.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FamilyTagServiceImplTest {

	@Mock
	private FamilyTagRepo familyTagRepo;
	@Mock
	private BenDetailRepo benDetailRepo;
	@Mock
	private BenMappingRepo benMappingRepo;

	@InjectMocks
	private FamilyTagServiceImpl service;

	private static final String FAMILY_ID = "1767225600000123";
	private static final Integer VAN_ID = 7;

	private MBeneficiarymapping mapping() {
		MBeneficiarymapping mapping = new MBeneficiarymapping();
		mapping.setBenDetailsId(BigInteger.valueOf(5L));
		mapping.setVanID(VAN_ID);
		return mapping;
	}

	private BenFamilyMapping family(Integer members, String headName) {
		BenFamilyMapping family = new BenFamilyMapping();
		family.setBenFamilyTagId(9L);
		family.setFamilyId(FAMILY_ID);
		family.setFamilyName("Devi");
		family.setNoOfmembers(members);
		family.setFamilyHeadName(headName);
		return family;
	}

	@Nested
	@DisplayName("tagging a beneficiary to a family")
	class Tagging {

		private static final String REQUEST = "{\"familyId\":\"1767225600000123\",\"beneficiaryRegId\":100200300,"
				+ "\"headofFamily_RelationID\":2,\"headofFamily_Relation\":\"Daughter\","
				+ "\"modifiedBy\":\"field.worker\"}";

		@Test
		@DisplayName("the family id is stamped on the beneficiary and the member count goes up")
		void familyIdIsStampedAndMemberCountIncrements() throws Exception {
			when(benMappingRepo.getBenDetailsId(BigInteger.valueOf(100200300L))).thenReturn(mapping());
			BenFamilyMapping family = family(3, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			String response = service.addTag(REQUEST);

			assertEquals("Family tagging completed successfully", response);
			verify(benDetailRepo).updateFamilyDetails(FAMILY_ID, 2, "Daughter", null, BigInteger.valueOf(5L),
					VAN_ID);
			assertEquals(4, family.getNoOfmembers());
			assertEquals("field.worker", family.getModifiedBy());
			verify(familyTagRepo).save(family);
		}

		@Test
		@DisplayName("the first member of a family with no recorded count sets it to one")
		void firstMemberSetsCountToOne() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			BenFamilyMapping family = family(null, null);
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			service.addTag(REQUEST);

			assertEquals(1, family.getNoOfmembers());
		}

		@Test
		@DisplayName("a member tagged as head of the family becomes the recorded head")
		void memberTaggedAsHeadBecomesTheRecordedHead() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			BenFamilyMapping family = family(1, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			service.addTag("{\"familyId\":\"" + FAMILY_ID + "\",\"beneficiaryRegId\":100200300,"
					+ "\"isHeadOfTheFamily\":true,\"memberName\":\"Ram Devi\"}");

			assertEquals("Ram Devi", family.getFamilyHeadName());
		}

		@Test
		@DisplayName("a beneficiary the platform does not know is rejected without touching the family")
		void unknownBeneficiaryIsRejected() {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(null);

			IEMRException thrown = assertThrows(IEMRException.class, () -> service.addTag(REQUEST));

			assertTrue(thrown.getMessage().contains("Beneficiary is not found"), thrown.getMessage());
			verify(familyTagRepo, never()).save(any());
		}

		@Test
		@DisplayName("a beneficiary with no sync key is rejected")
		void beneficiaryWithNoSyncKeyIsRejected() {
			MBeneficiarymapping incomplete = new MBeneficiarymapping();
			incomplete.setBenDetailsId(BigInteger.valueOf(5L));
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(incomplete);

			assertThrows(IEMRException.class, () -> service.addTag(REQUEST));
			verify(benDetailRepo, never()).updateFamilyDetails(anyString(), anyInt(), anyString(), anyString(),
					any(), anyInt());
		}

		@Test
		@DisplayName("an unknown family id is rejected after the beneficiary has been stamped")
		void unknownFamilyIdIsRejected() {
			// The stamp has already been written at this point, so the caller
			// has to retry with a valid family rather than assume nothing
			// happened.
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(null);

			IEMRException thrown = assertThrows(IEMRException.class, () -> service.addTag(REQUEST));

			assertTrue(thrown.getMessage().contains("Invalid family ID"), thrown.getMessage());
		}

		@Test
		@DisplayName("an unparseable request is rejected")
		void unparseableRequestIsRejected() {
			assertThrows(IEMRException.class, () -> service.addTag("not json at all {"));
		}
	}

	@Nested
	@DisplayName("untagging beneficiaries from a family")
	class Untagging {

		private static final String REQUEST = "{\"memberList\":[{\"familyId\":\"1767225600000123\","
				+ "\"beneficiaryRegId\":100200300,\"modifiedBy\":\"field.worker\"}]}";

		@Test
		@DisplayName("the beneficiary is untagged and the member count goes down")
		void beneficiaryIsUntaggedAndMemberCountDecrements() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			BenFamilyMapping family = family(3, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			String response = service.doFamilyUntag(REQUEST);

			assertEquals("Beneficiary untagged successfully", response);
			verify(benDetailRepo).untagFamily("field.worker", BigInteger.valueOf(5L), VAN_ID);
			assertEquals(2, family.getNoOfmembers());
		}

		@Test
		@DisplayName("the member count is not driven below zero")
		void memberCountIsNotDrivenBelowZero() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			BenFamilyMapping family = family(0, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			service.doFamilyUntag(REQUEST);

			assertEquals(0, family.getNoOfmembers());
		}

		@Test
		@DisplayName("untagging the head of the family leaves the family without a recorded head")
		void untaggingTheHeadClearsTheRecordedHead() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			BenFamilyMapping family = family(2, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);

			service.doFamilyUntag("{\"memberList\":[{\"familyId\":\"" + FAMILY_ID
					+ "\",\"beneficiaryRegId\":100200300,\"isHeadOfTheFamily\":true}]}");

			assertEquals("", family.getFamilyHeadName());
		}

		@Test
		@DisplayName("every member in the request is untagged")
		void everyMemberInTheRequestIsUntagged() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family(5, "Asha Devi"));

			service.doFamilyUntag("{\"memberList\":[{\"familyId\":\"" + FAMILY_ID
					+ "\",\"beneficiaryRegId\":100200300},{\"familyId\":\"" + FAMILY_ID
					+ "\",\"beneficiaryRegId\":100200301}]}");

			verify(benDetailRepo, org.mockito.Mockito.times(2)).untagFamily(any(), any(), anyInt());
		}

		@Test
		@DisplayName("a request with no member list is rejected")
		void requestWithNoMemberListIsRejected() {
			assertThrows(IEMRException.class, () -> service.doFamilyUntag("{}"));
		}

		@Test
		@DisplayName("a request with an empty member list is rejected")
		void requestWithEmptyMemberListIsRejected() {
			assertThrows(IEMRException.class, () -> service.doFamilyUntag("{\"memberList\":[]}"));
		}

		@Test
		@DisplayName("a member with no family id is rejected")
		void memberWithNoFamilyIdIsRejected() {
			IEMRException thrown = assertThrows(IEMRException.class,
					() -> service.doFamilyUntag("{\"memberList\":[{\"beneficiaryRegId\":100200300}]}"));

			assertTrue(thrown.getMessage().contains("Invalid family ID"), thrown.getMessage());
		}

		@Test
		@DisplayName("an unknown family id is rejected")
		void unknownFamilyIdIsRejected() {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(null);

			assertThrows(IEMRException.class, () -> service.doFamilyUntag(REQUEST));
		}
	}

	@Nested
	@DisplayName("editing a family")
	class Editing {

		private static final String REQUEST = "{\"familyId\":\"1767225600000123\",\"beneficiaryRegId\":100200300,"
				+ "\"headofFamily_RelationID\":3,\"headofFamily_Relation\":\"Son\"}";

		@Test
		@DisplayName("the beneficiary's relationship to the head is updated")
		void relationshipToTheHeadIsUpdated() throws Exception {
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family(2, "Asha Devi"));
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			String response = service.editFamilyDetails(REQUEST);

			assertEquals("Beneficiary family tagging updateed successfully", response);
			verify(benDetailRepo).editFamilyDetails(3, "Son", null, BigInteger.valueOf(5L), VAN_ID, FAMILY_ID);
		}

		@Test
		@DisplayName("promoting a member to head records their name")
		void promotingAMemberToHeadRecordsTheirName() throws Exception {
			BenFamilyMapping family = family(2, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			service.editFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\",\"beneficiaryRegId\":100200300,"
					+ "\"isHeadOfTheFamily\":true,\"memberName\":\"Ram Devi\"}");

			assertEquals("Ram Devi", family.getFamilyHeadName());
		}

		@Test
		@DisplayName("demoting the current head leaves the family without a recorded head")
		void demotingTheCurrentHeadClearsTheRecordedHead() throws Exception {
			BenFamilyMapping family = family(2, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			service.editFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\",\"beneficiaryRegId\":100200300,"
					+ "\"isHeadOfTheFamily\":false,\"memberName\":\"Asha Devi\"}");

			assertEquals("", family.getFamilyHeadName());
		}

		@Test
		@DisplayName("demoting someone who is not the head records them instead of clearing the head")
		void demotingSomeoneWhoIsNotTheHeadRecordsThem() throws Exception {
			BenFamilyMapping family = family(2, "Asha Devi");
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family);
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			service.editFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\",\"beneficiaryRegId\":100200300,"
					+ "\"isHeadOfTheFamily\":false,\"memberName\":\"Ram Devi\"}");

			assertEquals("Ram Devi", family.getFamilyHeadName());
		}

		@Test
		@DisplayName("an unknown family id is rejected")
		void unknownFamilyIdIsRejected() {
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(null);

			IEMRException thrown = assertThrows(IEMRException.class, () -> service.editFamilyDetails(REQUEST));

			assertTrue(thrown.getMessage().contains("Invalid Family ID"), thrown.getMessage());
		}

		@Test
		@DisplayName("an unknown beneficiary is rejected")
		void unknownBeneficiaryIsRejected() {
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family(2, "Asha Devi"));
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(null);

			assertThrows(IEMRException.class, () -> service.editFamilyDetails(REQUEST));
		}
	}

	@Nested
	@DisplayName("searching for families")
	class Searching {

		@Test
		@DisplayName("a search without a family id matches on name and village")
		void searchWithoutFamilyIdMatchesOnNameAndVillage() throws Exception {
			when(familyTagRepo.searchFamily("Devi", 401))
					.thenReturn(Collections.singletonList(family(3, "Asha Devi")));

			String response = service.searchFamily("{\"familyName\":\"Devi\",\"villageId\":401}");

			assertTrue(response.contains("Devi"), response);
			verify(familyTagRepo, never()).searchFamilyWithFamilyId(any(), any(), any());
		}

		@Test
		@DisplayName("a search with a family id narrows to that family")
		void searchWithFamilyIdNarrowsToThatFamily() throws Exception {
			when(familyTagRepo.searchFamilyWithFamilyId("Devi", 401, FAMILY_ID))
					.thenReturn(Collections.singletonList(family(3, "Asha Devi")));

			String response = service
					.searchFamily("{\"familyName\":\"Devi\",\"villageId\":401,\"familyId\":\"" + FAMILY_ID + "\"}");

			assertTrue(response.contains(FAMILY_ID), response);
			verify(familyTagRepo, never()).searchFamily(any(), any());
		}

		@Test
		@DisplayName("a search with no matches says so rather than returning an empty list")
		void searchWithNoMatchesSaysSo() throws Exception {
			when(familyTagRepo.searchFamily(any(), any())).thenReturn(Collections.emptyList());

			assertEquals("No records found", service.searchFamily("{\"familyName\":\"Nobody\"}"));
		}

		@Test
		@DisplayName("a failing search is reported")
		void failingSearchIsReported() {
			when(familyTagRepo.searchFamily(any(), any())).thenThrow(new IllegalStateException("table locked"));

			assertThrows(IEMRException.class, () -> service.searchFamily("{\"familyName\":\"Devi\"}"));
		}
	}

	@Nested
	@DisplayName("creating a family")
	class Creating {

		private static final String REQUEST = "{\"familyName\":\"Devi\",\"villageId\":401,"
				+ "\"beneficiaryRegId\":100200300,\"createdBy\":\"field.worker\"}";

		@Test
		@DisplayName("a new family gets a generated id, one member, and stamps its first beneficiary")
		void newFamilyGetsGeneratedIdAndStampsItsFirstBeneficiary() throws Exception {
			when(familyTagRepo.getUserId("field.worker")).thenReturn(123);
			when(familyTagRepo.save(any())).thenAnswer(invocation -> {
				BenFamilyMapping saved = invocation.getArgument(0);
				saved.setBenFamilyTagId(9L);
				return saved;
			});
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			String response = service.createFamily(REQUEST);

			assertTrue(response.contains("\"noOfmembers\":1"), response);
			ArgumentCaptor<BenFamilyMapping> captor = ArgumentCaptor.forClass(BenFamilyMapping.class);
			verify(familyTagRepo).save(captor.capture());
			assertEquals(16, captor.getValue().getFamilyId().length(),
					"a family id is 13 timestamp digits plus 3 user digits");
			assertTrue(captor.getValue().getFamilyId().endsWith("123"));
			verify(benDetailRepo).updateFamilyDetails(eq(captor.getValue().getFamilyId()), any(), any(), any(),
					eq(BigInteger.valueOf(5L)), eq(VAN_ID));
		}

		@Test
		@DisplayName("a short user id is padded so every family id is the same length")
		void shortUserIdIsPadded() throws Exception {
			when(familyTagRepo.getUserId("field.worker")).thenReturn(7);
			when(familyTagRepo.save(any())).thenAnswer(invocation -> {
				BenFamilyMapping saved = invocation.getArgument(0);
				saved.setBenFamilyTagId(9L);
				return saved;
			});
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			service.createFamily(REQUEST);

			ArgumentCaptor<BenFamilyMapping> captor = ArgumentCaptor.forClass(BenFamilyMapping.class);
			verify(familyTagRepo).save(captor.capture());
			assertEquals(16, captor.getValue().getFamilyId().length());
			assertTrue(captor.getValue().getFamilyId().endsWith("700"));
		}

		@Test
		@DisplayName("an unrecognised creating user is rejected")
		void unrecognisedCreatingUserIsRejected() {
			when(familyTagRepo.getUserId("field.worker")).thenReturn(null);

			assertThrows(IEMRException.class, () -> service.createFamily(REQUEST));
			verify(familyTagRepo, never()).save(any());
		}

		@Test
		@DisplayName("a family the database did not assign an id to is reported as a failure")
		void familyWithNoAssignedIdIsReportedAsFailure() {
			when(familyTagRepo.getUserId("field.worker")).thenReturn(123);
			when(familyTagRepo.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());

			assertThrows(IEMRException.class, () -> service.createFamily(REQUEST));
		}

		@Test
		@DisplayName("a beneficiary that cannot be resolved leaves the family created but unstamped")
		void unresolvableBeneficiaryLeavesTheFamilyUnstamped() throws Exception {
			when(familyTagRepo.getUserId("field.worker")).thenReturn(123);
			when(familyTagRepo.save(any())).thenAnswer(invocation -> {
				BenFamilyMapping saved = invocation.getArgument(0);
				saved.setBenFamilyTagId(9L);
				return saved;
			});
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(null);

			assertNotNull(service.createFamily(REQUEST));

			verify(benDetailRepo, never()).updateFamilyDetails(anyString(), anyInt(), anyString(), anyString(),
					any(), anyInt());
		}
	}

	@Nested
	@DisplayName("listing family members")
	class ListingMembers {

		private MBeneficiarydetail member(String title, String firstName, String lastName, String relation,
				String other) {
			MBeneficiarydetail detail = new MBeneficiarydetail();
			detail.setBeneficiaryDetailsId(BigInteger.valueOf(5L));
			detail.setVanID(VAN_ID);
			detail.setTitle(title);
			detail.setFirstName(firstName);
			detail.setLastName(lastName);
			detail.setHeadOfFamily_Relation(relation);
			detail.setOther(other);
			detail.setFamilyId(FAMILY_ID);
			return detail;
		}

		@Test
		@DisplayName("each member is listed with its registration id and assembled name")
		void eachMemberIsListedWithIdAndName() throws Exception {
			when(benDetailRepo.getFamilyDetails(FAMILY_ID))
					.thenReturn(Collections.singletonList(member("Ms", "Asha", "Devi", "Daughter", null)));
			when(benMappingRepo.getBenRegId(BigInteger.valueOf(5L), VAN_ID))
					.thenReturn(BigInteger.valueOf(100200300L));

			String response = service.getFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\"}");

			assertTrue(response.contains("Ms Asha Devi"), response);
			assertTrue(response.contains("100200300"), response);
			assertTrue(response.contains("Daughter"), response);
		}

		@Test
		@DisplayName("a member with missing name parts is still listed")
		void memberWithMissingNamePartsIsStillListed() throws Exception {
			when(benDetailRepo.getFamilyDetails(FAMILY_ID))
					.thenReturn(Collections.singletonList(member(null, null, null, null, null)));
			when(benMappingRepo.getBenRegId(any(), anyInt())).thenReturn(null);

			assertNotNull(service.getFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\"}"));
		}

		@Test
		@DisplayName("a free-text relationship is appended to the relationship label")
		void freeTextRelationshipIsAppended() throws Exception {
			when(benDetailRepo.getFamilyDetails(FAMILY_ID))
					.thenReturn(Collections.singletonList(member("Mr", "Ram", "Devi", "Other", "Nephew")));
			when(benMappingRepo.getBenRegId(any(), anyInt())).thenReturn(BigInteger.valueOf(100200301L));

			assertTrue(service.getFamilyDetails("{\"familyId\":\"" + FAMILY_ID + "\"}")
					.contains("Other-Nephew"));
		}

		@Test
		@DisplayName("a request with no family id is rejected")
		void requestWithNoFamilyIdIsRejected() {
			assertThrows(IEMRException.class, () -> service.getFamilyDetails("{}"));
		}

		@Test
		@DisplayName("a beneficiary's own family is returned with the family summary and its members")
		void beneficiarysOwnFamilyIsReturnedWithSummaryAndMembers() throws Exception {
			when(benMappingRepo.getBenDetailsId(BigInteger.valueOf(100200300L))).thenReturn(mapping());
			when(benDetailRepo.findByBeneficiaryDetailsIdOrderByBeneficiaryDetailsIdAsc(BigInteger.valueOf(5L)))
					.thenReturn(Collections.singletonList(member("Ms", "Asha", "Devi", "Daughter", null)));
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(family(2, "Ram Devi"));
			when(benDetailRepo.getFamilyDetails(FAMILY_ID))
					.thenReturn(Arrays.asList(member("Ms", "Asha", "Devi", "Daughter", null),
							member("Mr", "Ram", "Devi", "Self", null)));
			when(benMappingRepo.getBenRegId(any(), anyInt())).thenReturn(BigInteger.valueOf(100200300L));

			String response = service.getFamilyDetailsByBeneficiaryId("{\"beneficiaryRegId\":100200300}");

			assertTrue(response.contains(FAMILY_ID), response);
			assertTrue(response.contains("Ram Devi"), response);
			assertTrue(response.contains("Asha Devi"), response);
			assertTrue(response.contains("\"noOfMembers\":2"), response);
		}

		@Test
		@DisplayName("a beneficiary with no family tagged says so rather than returning an empty family")
		void beneficiaryWithNoFamilySaysSo() throws Exception {
			MBeneficiarydetail untagged = member("Ms", "Asha", "Devi", null, null);
			untagged.setFamilyId(null);
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(benDetailRepo.findByBeneficiaryDetailsIdOrderByBeneficiaryDetailsIdAsc(any()))
					.thenReturn(Collections.singletonList(untagged));

			assertEquals("No family tagged to this beneficiary",
					service.getFamilyDetailsByBeneficiaryId("{\"beneficiaryRegId\":100200300}"));
		}

		@Test
		@DisplayName("a beneficiary with no detail row says no family is tagged")
		void beneficiaryWithNoDetailRowSaysNoFamily() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(benDetailRepo.findByBeneficiaryDetailsIdOrderByBeneficiaryDetailsIdAsc(any()))
					.thenReturn(Collections.emptyList());

			assertEquals("No family tagged to this beneficiary",
					service.getFamilyDetailsByBeneficiaryId("{\"beneficiaryRegId\":100200300}"));
		}

		@Test
		@DisplayName("a request with no registration id is rejected")
		void requestWithNoRegistrationIdIsRejected() {
			IEMRException thrown = assertThrows(IEMRException.class,
					() -> service.getFamilyDetailsByBeneficiaryId("{}"));

			assertTrue(thrown.getMessage().contains("beneficiaryRegId is required"), thrown.getMessage());
		}

		@Test
		@DisplayName("an unknown beneficiary is rejected")
		void unknownBeneficiaryIsRejected() {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(null);

			IEMRException thrown = assertThrows(IEMRException.class,
					() -> service.getFamilyDetailsByBeneficiaryId("{\"beneficiaryRegId\":100200300}"));

			assertTrue(thrown.getMessage().contains("Beneficiary not found"), thrown.getMessage());
		}

		@Test
		@DisplayName("a family whose master row is missing still lists the members")
		void familyWithMissingMasterRowStillListsMembers() throws Exception {
			when(benMappingRepo.getBenDetailsId(any())).thenReturn(mapping());
			when(benDetailRepo.findByBeneficiaryDetailsIdOrderByBeneficiaryDetailsIdAsc(any()))
					.thenReturn(Collections.singletonList(member("Ms", "Asha", "Devi", "Daughter", null)));
			when(familyTagRepo.searchFamilyByFamilyId(FAMILY_ID)).thenReturn(null);
			when(benDetailRepo.getFamilyDetails(FAMILY_ID))
					.thenReturn(Collections.singletonList(member("Ms", "Asha", "Devi", "Daughter", null)));

			assertTrue(service.getFamilyDetailsByBeneficiaryId("{\"beneficiaryRegId\":100200300}")
					.contains("Asha Devi"));
		}
	}
}
