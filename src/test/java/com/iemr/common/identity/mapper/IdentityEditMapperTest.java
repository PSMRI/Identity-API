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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.iemr.common.identity.domain.Address;
import com.iemr.common.identity.domain.Contact;
import com.iemr.common.identity.domain.Identity;
import com.iemr.common.identity.domain.MBeneficiaryaddress;
import com.iemr.common.identity.domain.MBeneficiaryconsent;
import com.iemr.common.identity.domain.MBeneficiarycontact;
import com.iemr.common.identity.domain.MBeneficiaryfamilymapping;
import com.iemr.common.identity.domain.MBeneficiaryidentity;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.IdentityEditDTO;
import com.iemr.common.identity.fixture.PojoFixture;

/**
 * Field-level tests for the beneficiary-edit mapper.
 *
 * <p>
 * The edit flow flattens a nested {@link IdentityEditDTO} - three addresses, a
 * contact block, identity and family lists - onto the wide {@code m_beneficiary*}
 * tables, and the column each field lands in is not obvious from the names. These
 * tests assert the mappings that a rename or a reordered {@code @Mapping} would
 * silently break; the null-guard sweep lives in {@link MapperContractTest}.
 */
class IdentityEditMapperTest {

	private final IdentityEditMapper mapper = IdentityEditMapper.INSTANCE;

	private Address address(String line1, String city, Integer stateId) {
		Address address = new Address();
		address.setAddrLine1(line1);
		address.setAddrLine2(city);
		address.setStateId(stateId);
		address.setState("Karnataka");
		address.setDistrictId(200);
		address.setDistrict("Bengaluru");
		address.setSubDistrictId(300);
		address.setSubDistrict("North");
		address.setVillageId(400);
		address.setVillage("Yelahanka");
		address.setPinCode("560064");
		address.setHabitation("Main");
		address.setAddressValue(line1 + ", " + city);
		return address;
	}

	@Test
	@DisplayName("agent name and event date become the audit columns on the mapping row")
	void mappingRowTakesAuditColumnsFromAgentFields() {
		IdentityEditDTO dto = new IdentityEditDTO();
		dto.setAgentName("field.worker");
		dto.setEventTypeDate(Timestamp.valueOf("2026-03-01 10:15:00"));
		dto.setBenAccountID(BigInteger.valueOf(77L));
		dto.setBenImageId(42L);
		dto.setVanID(9);

		MBeneficiarymapping mapping = mapper.identityEditDTOToMBeneficiarymapping(dto);

		assertEquals("field.worker", mapping.getCreatedBy());
		assertEquals(Timestamp.valueOf("2026-03-01 10:15:00"), mapping.getLastModDate());
		assertEquals(BigInteger.valueOf(77L), mapping.getBenAccountID());
		assertEquals(BigInteger.valueOf(42L), mapping.getBenImageId());
		assertEquals(9, mapping.getVanID());
	}

	@Test
	@DisplayName("a null benImageId leaves the image column unset instead of failing to convert")
	void nullBenImageIdIsNotConverted() {
		IdentityEditDTO dto = new IdentityEditDTO();
		dto.setAgentName("field.worker");

		MBeneficiarymapping mapping = mapper.identityEditDTOToMBeneficiarymapping(dto);

		assertNull(mapping.getBenImageId());
	}

	@Nested
	@DisplayName("address flattening")
	class AddressFlattening {

		@Test
		@DisplayName("current, permanent and emergency addresses land in their own column prefixes")
		void eachAddressLandsInItsOwnPrefix() {
			IdentityEditDTO dto = new IdentityEditDTO();
			dto.setCurrentAddress(address("1 Current St", "Bengaluru", 101));
			dto.setPermanentAddress(address("2 Permanent Rd", "Mysuru", 102));
			dto.setEmergencyAddress(address("3 Emergency Ln", "Hubballi", 103));

			MBeneficiaryaddress mapped = mapper.identityEditDTOToMBeneficiaryaddress(dto);

			assertEquals("1 Current St", mapped.getCurrAddrLine1());
			assertEquals("2 Permanent Rd", mapped.getPermAddrLine1());
			assertEquals("3 Emergency Ln", mapped.getEmerAddrLine1());
			assertEquals(101, mapped.getCurrStateId());
			assertEquals(102, mapped.getPermStateId());
			assertEquals(103, mapped.getEmerStateId());
			assertEquals("560064", mapped.getCurrPinCode());
			assertEquals("Yelahanka", mapped.getEmerVillage());
		}

		@Test
		@DisplayName("the mapper requires all three addresses to be present")
		void missingNestedAddressIsRejected() {
			// MBeneficiaryaddress.setCurrentAddress dereferences its argument
			// without a null check and the generated mapper calls it
			// unconditionally, so an edit payload that omits an address block
			// fails here rather than persisting a half-filled row. Callers in
			// IdentityService populate all three before mapping.
			IdentityEditDTO dto = new IdentityEditDTO();
			dto.setFirstName("Asha");

			assertThrows(NullPointerException.class, () -> mapper.identityEditDTOToMBeneficiaryaddress(dto));
		}
	}

	@Test
	@DisplayName("consent defaults are applied per share-scope rather than copied wholesale")
	void consentDefaultsAreAppliedPerScope() {
		IdentityEditDTO dto = new IdentityEditDTO();
		dto.setAgentName("field.worker");

		MBeneficiaryconsent consent = mapper.identityEditDTOToDefaultMBeneficiaryconsent(dto, Boolean.TRUE,
				Boolean.FALSE);

		assertNotNull(consent);
		assertTrue(consent.getShareMedicalDetailsWithDoctor());
		assertTrue(consent.getSharePersonalDetailsWithSpouse());
		assertFalse(consent.getShareAnonymousWithGovt());
		assertFalse(consent.getSharePersonalDetailsForMedicalStudy());
	}

	@Test
	@DisplayName("contact numbers are copied into the numbered phone columns")
	void contactNumbersPopulateNumberedColumns() {
		IdentityEditDTO dto = new IdentityEditDTO();
		Contact contact = new Contact();
		contact.setPreferredPhoneNum("9000000001");
		contact.setPhoneNum1("9000000002");
		contact.setPhoneNum2("9000000003");
		contact.setEmergencyContactNum("9000000004");
		dto.setContact(contact);
		dto.setPreferredEmailId("asha@example.org");

		MBeneficiarycontact mapped = mapper.identityEdiDTOToMBeneficiarycontact(dto);

		assertEquals("9000000001", mapped.getPreferredPhoneNum());
		assertEquals("9000000002", mapped.getPhoneNum1());
		assertEquals("9000000003", mapped.getPhoneNum2());
		assertEquals("9000000004", mapped.getEmergencyContactNum());
		assertEquals("asha@example.org", mapped.getEmailId());
	}

	@Test
	@DisplayName("family mapping carries the supplied audit fields, not the DTO's own")
	void familyMappingUsesSuppliedAuditFields() {
		BenFamilyDTO family = new BenFamilyDTO();
		family.setAssociatedBenRegId(BigInteger.valueOf(555L));
		Timestamp created = Timestamp.valueOf("2026-01-02 03:04:05");

		MBeneficiaryfamilymapping mapped = mapper.identityEditDTOToMBeneficiaryfamilymapping(family, "supervisor",
				created);

		assertEquals(BigInteger.valueOf(555L), mapped.getAssociatedBenRegId());
		assertEquals("supervisor", mapped.getCreatedBy());
		assertEquals(created, mapped.getCreatedDate());
	}

	@Test
	@DisplayName("identity rows carry the supplied audit fields")
	void identityRowUsesSuppliedAuditFields() {
		Identity identity = new Identity();
		identity.setIdentityNo("ABHA-1234");
		identity.setIdentityName("ABHA");
		Timestamp created = Timestamp.valueOf("2026-01-02 03:04:05");

		MBeneficiaryidentity mapped = mapper.identityToMBeneficiaryidentity(identity, "supervisor", created);

		assertEquals("ABHA-1234", mapped.getIdentityNo());
		assertEquals("supervisor", mapped.getCreatedBy());
		assertEquals(created, mapped.getCreatedDate());
	}

	@Test
	@DisplayName("list mappings preserve order and size")
	void listMappingsPreserveOrderAndSize() {
		BenFamilyDTO first = new BenFamilyDTO();
		first.setAssociatedBenRegId(BigInteger.ONE);
		BenFamilyDTO second = new BenFamilyDTO();
		second.setAssociatedBenRegId(BigInteger.TEN);

		List<MBeneficiaryfamilymapping> mapped = mapper
				.identityEditDTOListToMBeneficiaryfamilymappingList(Arrays.asList(first, second));

		assertEquals(2, mapped.size());
		assertEquals(BigInteger.ONE, mapped.get(0).getAssociatedBenRegId());
		assertEquals(BigInteger.TEN, mapped.get(1).getAssociatedBenRegId());
	}

	@Test
	@DisplayName("an empty list maps to an empty list, not null")
	void emptyListMapsToEmptyList() {
		assertTrue(mapper.identityEditDTOListToMBeneficiaryfamilymappingList(Collections.emptyList()).isEmpty());
		assertTrue(mapper.identityEditDTOListToMBeneficiaryidentityList(Collections.emptyList()).isEmpty());
		assertTrue(mapper.mBeneficiaryfamilymappingListToBenFamilyDTOList(Collections.emptyList()).isEmpty());
	}

	@Test
	@DisplayName("account and image mappings accept a list of edit payloads")
	void accountAndImageListMappings() {
		IdentityEditDTO dto = PojoFixture.populate(IdentityEditDTO.class);

		assertEquals(1, mapper.identityEditDTOToMBeneficiaryAccount(Collections.singletonList(dto)).size());
		assertEquals(1, mapper.identityEditDTOToMBeneficiaryImage(Collections.singletonList(dto)).size());
	}
}
