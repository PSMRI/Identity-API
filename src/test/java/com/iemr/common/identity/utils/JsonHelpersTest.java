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
package com.iemr.common.identity.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.iemr.common.identity.dto.BenIdImportDTO;
import com.iemr.common.identity.filter.QuerySelector;
import com.iemr.common.identity.dto.IdentityFilterDTO;

import java.math.BigInteger;

/**
 * Tests for the small JSON and filter helpers.
 *
 * <p>
 * {@link Utilities#getJsonAsString} is used to log request payloads, so it has
 * to return something rather than throw on an object Jackson cannot handle -
 * losing a log line is acceptable, failing the request that produced it is not.
 * {@link IdentityFilterDTO} wraps its fields in {@link java.util.Optional}, and
 * {@link QuerySelector} reads them, so both are exercised together.
 */
class JsonHelpersTest {

	@Test
	@DisplayName("an object is serialised to JSON for logging")
	void objectIsSerialisedToJsonForLogging() {
		BenIdImportDTO dto = new BenIdImportDTO();
		dto.setBenRegId(BigInteger.valueOf(100200300L));
		dto.setBeneficiaryId(BigInteger.valueOf(4001L));

		String json = new Utilities().getJsonAsString(dto);

		assertTrue(json.contains("100200300"), json);
		assertTrue(json.contains("4001"), json);
	}

	@Test
	@DisplayName("an object Jackson cannot serialise yields an empty string rather than throwing")
	void unserialisableObjectYieldsAnEmptyString() {
		assertEquals("", new Utilities().getJsonAsString(new Object() {
			@SuppressWarnings("unused")
			public String getBoom() {
				throw new IllegalStateException("cannot serialise");
			}
		}));
	}

	@Test
	@DisplayName("the null-stripping helper is a no-op placeholder")
	void nullStrippingHelperIsANoOpPlaceholder() {
		// Retained for callers that expect the hook; it currently only logs.
		new JsonUtilities().removeNullsFromJson("{\"firstName\":null}");
	}

	@Test
	@DisplayName("an unset filter field reads as null, not as an empty optional")
	void unsetFilterFieldReadsAsNull() {
		// The Optional-typed fields are never initialised, so callers still have
		// to null-check them - which is what QuerySelector does not do.
		IdentityFilterDTO filter = new IdentityFilterDTO();

		org.junit.jupiter.api.Assertions.assertNull(filter.getBeneficiaryId());
		org.junit.jupiter.api.Assertions.assertNull(filter.getFirstName());
	}

	@Test
	@DisplayName("a set filter field is readable through its optional")
	void setFilterFieldIsReadableThroughItsOptional() {
		IdentityFilterDTO filter = new IdentityFilterDTO();

		filter.setBeneficiaryId(BigInteger.valueOf(4001L));
		filter.setBeneficiaryRegId(BigInteger.valueOf(100200300L));
		filter.setFirstName("Asha");
		filter.setMiddleName("Rani");
		filter.setLastName("Devi");
		filter.setAgeId(3);
		filter.setAge(30);
		filter.setGenderId(2);
		filter.setGenderName("Female");
		filter.setSpouseName("Suresh");
		filter.setFatherName("Ram");
		filter.setPinCode("560064");
		filter.setContactNumber("9000000000");

		assertEquals(BigInteger.valueOf(4001L), filter.getBeneficiaryId().get());
		assertEquals("Asha", filter.getFirstName().get());
		assertEquals("560064", filter.getPinCode().get());
		assertEquals("9000000000", filter.getContactNumber().get());
	}

	@Test
	@DisplayName("the query selector reads every criterion the filter can carry")
	void querySelectorReadsEveryCriterion() {
		IdentityFilterDTO filter = new IdentityFilterDTO();
		filter.setBeneficiaryId(BigInteger.valueOf(4001L));
		filter.setBeneficiaryRegId(BigInteger.valueOf(100200300L));
		filter.setFirstName("Asha");
		filter.setPinCode("560064");
		filter.setContactNumber("9000000000");

		new QuerySelector().getQuery(filter);
	}

	@Test
	@DisplayName("the query selector requires every criterion to have been set")
	void querySelectorRequiresEveryCriterionToHaveBeenSet() {
		// It reads each field's optional without a null check, so a filter with
		// an unset field fails rather than being treated as unconstrained.
		org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
				() -> new QuerySelector().getQuery(new IdentityFilterDTO()));
	}
}
