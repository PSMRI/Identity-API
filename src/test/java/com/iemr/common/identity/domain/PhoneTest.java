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
package com.iemr.common.identity.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the fan-out from the six flat phone columns on
 * {@link MBeneficiarycontact} to the {@link Phone} list the API returns.
 */
class PhoneTest {

	@Test
	@DisplayName("each populated phone column becomes one entry, in column order")
	void populatedColumnsBecomeEntriesInColumnOrder() {
		MBeneficiarycontact contact = new MBeneficiarycontact();
		contact.setPreferredPhoneNum("9000000000");
		contact.setPreferredPhoneTyp("Mobile");
		contact.setPhoneNum1("9000000001");
		contact.setPhoneTyp1("Landline");
		contact.setPhoneNum5("9000000005");
		contact.setPhoneTyp5("Work");

		List<Phone> phones = Phone.createContactList(contact, "12345", "Asha Devi");

		assertEquals(3, phones.size());
		assertEquals("9000000000", phones.get(0).getPhoneNum());
		assertEquals("Mobile", phones.get(0).getPhoneType());
		assertEquals("9000000001", phones.get(1).getPhoneNum());
		assertEquals("9000000005", phones.get(2).getPhoneNum());
	}

	@Test
	@DisplayName("every entry is stamped with the owning beneficiary")
	void everyEntryIsStampedWithTheOwner() {
		MBeneficiarycontact contact = new MBeneficiarycontact();
		contact.setPhoneNum2("9000000002");
		contact.setPhoneNum3("9000000003");
		contact.setPhoneNum4("9000000004");

		List<Phone> phones = Phone.createContactList(contact, "12345", "Asha Devi");

		assertEquals(3, phones.size());
		phones.forEach(phone -> {
			assertEquals("12345", phone.getBelongsToBenRegId());
			assertEquals("Asha Devi", phone.getBelongsToName());
		});
	}

	@Test
	@DisplayName("a contact row with no numbers yields no list at all")
	void contactWithNoNumbersYieldsNoList() {
		// Callers distinguish "no contact details recorded" from "an empty set of
		// numbers", so this returns null rather than an empty list.
		assertNull(Phone.createContactList(new MBeneficiarycontact(), "12345", "Asha Devi"));
	}
}
