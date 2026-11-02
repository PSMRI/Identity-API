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
package com.iemr.common.identity.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Elasticsearch-to-API response shape.
 *
 * <p>
 * The 1097 call-centre UI reads a nested structure that predates the search
 * index - demographics wrapped in {@code i_bendemographics}, with separate
 * {@code m_state}/{@code m_district}/{@code m_districtblock} objects, and phone
 * numbers as {@code benPhoneMaps}. Index documents are flat, so this mapper
 * rebuilds that nesting. A field dropped here is a blank in the agent's screen
 * with nothing in the logs, which is what these tests are for.
 */
class BeneficiaryESMapperTest {

	private final BeneficiaryESMapper mapper = new BeneficiaryESMapper();

	private Map<String, Object> indexDocument() {
		Map<String, Object> document = new HashMap<>();
		document.put("beneficiaryRegID", 100200300L);
		document.put("beneficiaryID", "4001");
		document.put("firstName", "Asha");
		document.put("lastName", "Devi");
		document.put("genderID", 2);
		document.put("genderName", "Female");
		document.put("dOB", "1996-01-15");
		document.put("age", 30);
		document.put("createdBy", "field.worker");
		document.put("createdDate", "2026-01-01");
		document.put("lastModDate", 1767225600000L);
		document.put("benAccountID", 77L);
		return document;
	}

	private Map<String, Object> demographics() {
		Map<String, Object> demographics = new HashMap<>();
		demographics.put("stateID", 101);
		demographics.put("stateName", "Karnataka");
		demographics.put("stateCode", "KA");
		demographics.put("districtID", 201);
		demographics.put("districtName", "Bengaluru");
		demographics.put("blockID", 301);
		demographics.put("blockName", "North");
		demographics.put("districtBranchID", 401);
		demographics.put("districtBranchName", "Yelahanka");
		demographics.put("pinCode", "560064");
		return demographics;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> firstBeneficiary(Map<String, Object> response) {
		return ((List<Map<String, Object>>) response.get("data")).get(0);
	}

	@Test
	@DisplayName("the envelope reports success around the transformed data")
	void envelopeReportsSuccess() {
		Map<String, Object> response = mapper
				.transformESResponse(Collections.singletonList(indexDocument()));

		assertEquals(200, response.get("statusCode"));
		assertEquals("Success", response.get("status"));
		assertEquals("Success", response.get("errorMessage"));
		assertEquals(1, ((List<?>) response.get("data")).size());
	}

	@Test
	@DisplayName("no hits yields an empty data list rather than a missing key")
	void noHitsYieldsEmptyDataList() {
		Map<String, Object> response = mapper.transformESResponse(Collections.emptyList());

		assertNotNull(response.get("data"));
		assertTrue(((List<?>) response.get("data")).isEmpty());
	}

	@Test
	@DisplayName("every hit is transformed, not just the first")
	void everyHitIsTransformed() {
		Map<String, Object> second = indexDocument();
		second.put("firstName", "Sunita");

		Map<String, Object> response = mapper
				.transformESResponse(Arrays.asList(indexDocument(), second));

		assertEquals(2, ((List<?>) response.get("data")).size());
	}

	@Test
	@DisplayName("the identity fields the agent's screen reads are all carried across")
	void identityFieldsAreCarriedAcross() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		assertEquals(100200300L, beneficiary.get("beneficiaryRegID"));
		assertEquals("4001", beneficiary.get("beneficiaryID"));
		assertEquals("Asha", beneficiary.get("firstName"));
		assertEquals("Devi", beneficiary.get("lastName"));
		assertEquals(2, beneficiary.get("genderID"));
		assertEquals("Female", beneficiary.get("genderName"));
		assertEquals("field.worker", beneficiary.get("createdBy"));
		assertEquals(1767225600000L, beneficiary.get("lastModDate"));
		assertEquals(77L, beneficiary.get("benAccountID"));
	}

	@Test
	@DisplayName("the date of birth and age are published under both names the UI uses")
	void dateOfBirthAndAgeArePublishedUnderBothNames() {
		// The screen reads dOB in one place and dob in another; dropping either
		// blanks a field.
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		assertEquals("1996-01-15", beneficiary.get("dOB"));
		assertEquals("1996-01-15", beneficiary.get("dob"));
		assertEquals(30, beneficiary.get("age"));
		assertEquals(30, beneficiary.get("actualAge"));
		assertEquals("Years", beneficiary.get("ageUnits"));
	}

	@Test
	@DisplayName("absent optional text fields become blanks rather than nulls")
	void absentOptionalTextFieldsBecomeBlanks() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		assertEquals("", beneficiary.get("fatherName"));
		assertEquals("", beneficiary.get("spouseName"));
		assertEquals("", beneficiary.get("isHIVPos"));
	}

	@Test
	@DisplayName("present optional text fields are carried across unchanged")
	void presentOptionalTextFieldsAreCarriedAcross() {
		Map<String, Object> document = indexDocument();
		document.put("fatherName", "Ram");
		document.put("spouseName", "Suresh");
		document.put("isHIVPos", "no");

		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(document)));

		assertEquals("Ram", beneficiary.get("fatherName"));
		assertEquals("Suresh", beneficiary.get("spouseName"));
		assertEquals("no", beneficiary.get("isHIVPos"));
	}

	@Test
	@DisplayName("gender is published both flat and as the nested object the UI binds to")
	void genderIsPublishedFlatAndNested() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		@SuppressWarnings("unchecked")
		Map<String, Object> gender = (Map<String, Object>) beneficiary.get("m_gender");
		assertEquals(2, gender.get("genderID"));
		assertEquals("Female", gender.get("genderName"));
	}

	@Test
	@DisplayName("demographics are rebuilt into the nested location objects the UI expects")
	void demographicsAreRebuiltIntoNestedObjects() {
		Map<String, Object> document = indexDocument();
		document.put("demographics", demographics());

		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(document)));

		@SuppressWarnings("unchecked")
		Map<String, Object> nested = (Map<String, Object>) beneficiary.get("i_bendemographics");
		assertEquals(100200300L, nested.get("beneficiaryRegID"));
		assertEquals(101, nested.get("stateID"));

		@SuppressWarnings("unchecked")
		Map<String, Object> state = (Map<String, Object>) nested.get("m_state");
		assertEquals(101, state.get("stateID"));
		assertEquals("Karnataka", state.get("stateName"));
		assertEquals("KA", state.get("stateCode"));
		assertEquals(1, state.get("countryID"));

		@SuppressWarnings("unchecked")
		Map<String, Object> district = (Map<String, Object>) nested.get("m_district");
		assertEquals(201, district.get("districtID"));
		assertEquals("Bengaluru", district.get("districtName"));
		assertEquals(101, district.get("stateID"));

		@SuppressWarnings("unchecked")
		Map<String, Object> block = (Map<String, Object>) nested.get("m_districtblock");
		assertEquals(301, block.get("blockID"));
		assertEquals("North", block.get("blockName"));
		assertEquals(201, block.get("districtID"));

		@SuppressWarnings("unchecked")
		Map<String, Object> branch = (Map<String, Object>) nested.get("m_districtbranchmapping");
		assertEquals(401, branch.get("districtBranchID"));
		assertEquals("Yelahanka", branch.get("villageName"));
		assertEquals("560064", branch.get("pinCode"));
	}

	@Test
	@DisplayName("a hit with no demographics omits the nested block rather than publishing an empty one")
	void hitWithNoDemographicsOmitsTheNestedBlock() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		assertFalse(beneficiary.containsKey("i_bendemographics"));
	}

	@Test
	@DisplayName("phone numbers are rebuilt as phone maps with a nested relationship object")
	void phoneNumbersAreRebuiltAsPhoneMaps() {
		Map<String, Object> phone = new HashMap<>();
		phone.put("phoneNo", "9000000000");
		phone.put("benRelationshipID", 1);
		phone.put("benRelationshipType", "Self");
		Map<String, Object> document = indexDocument();
		document.put("phoneNumbers", Collections.singletonList(phone));

		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(document)));

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> phoneMaps = (List<Map<String, Object>>) beneficiary.get("benPhoneMaps");
		assertEquals(1, phoneMaps.size());
		assertEquals("9000000000", phoneMaps.get(0).get("phoneNo"));
		assertEquals(100200300L, phoneMaps.get(0).get("benificiaryRegID"));

		@SuppressWarnings("unchecked")
		Map<String, Object> relation = (Map<String, Object>) phoneMaps.get(0).get("benRelationshipType");
		assertEquals(1, relation.get("benRelationshipID"));
		assertEquals("Self", relation.get("benRelationshipType"));
	}

	@Test
	@DisplayName("a hit with no phone numbers gets an empty phone-map list")
	void hitWithNoPhoneNumbersGetsAnEmptyList() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		assertTrue(((List<?>) beneficiary.get("benPhoneMaps")).isEmpty());
	}

	@Test
	@DisplayName("an empty phone-number list also yields an empty phone-map list")
	void emptyPhoneNumberListYieldsAnEmptyList() {
		Map<String, Object> document = indexDocument();
		document.put("phoneNumbers", new ArrayList<>());

		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(document)));

		assertTrue(((List<?>) beneficiary.get("benPhoneMaps")).isEmpty());
	}

	@Test
	@DisplayName("the change-tracking flags the edit screen posts back all start false")
	void changeTrackingFlagsAllStartFalse() {
		// The screen posts this object back on save; a flag left true would
		// trigger a spurious update of that section.
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(indexDocument())));

		for (String flag : new String[] { "isConsent", "changeInSelfDetails", "changeInAddress", "changeInContacts",
				"changeInIdentities", "changeInOtherDetails", "changeInFamilyDetails", "changeInAssociations",
				"changeInBankDetails", "changeInBenImage", "is1097", "emergencyRegistration", "passToNurse" }) {
			assertEquals(false, beneficiary.get(flag), flag + " should default to false");
		}
		assertTrue(((Map<?, ?>) beneficiary.get("m_title")).isEmpty());
		assertTrue(((Map<?, ?>) beneficiary.get("maritalStatus")).isEmpty());
		assertTrue(((List<?>) beneficiary.get("beneficiaryIdentities")).isEmpty());
	}

	@Test
	@DisplayName("a hit missing every field still produces a usable envelope")
	void hitMissingEveryFieldStillProducesAUsableEnvelope() {
		Map<String, Object> beneficiary = firstBeneficiary(
				mapper.transformESResponse(Collections.singletonList(new HashMap<>())));

		assertNull(beneficiary.get("firstName"));
		assertEquals("Years", beneficiary.get("ageUnits"));
		assertEquals("", beneficiary.get("fatherName"));
	}
}
