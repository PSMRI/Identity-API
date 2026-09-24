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
package com.iemr.common.identity.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ordering the search endpoints sort their results by.
 *
 * <p>
 * Both result DTOs are {@link Comparable} and every search endpoint calls
 * {@code Collections.sort} before responding, so the agent sees a stable order
 * across repeated searches. The two types order on different keys, and neither
 * tolerates a null key - which is why the endpoints filter nulls out of the
 * list first.
 */
class BeneficiaryOrderingTest {

	private BeneficiariesDTO beneficiary(long benMapId) {
		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenMapId(BigInteger.valueOf(benMapId));
		return dto;
	}

	private BeneficiariesPartialDTO partial(long benRegId) {
		BeneficiariesPartialDTO dto = new BeneficiariesPartialDTO();
		dto.setBenRegId(BigInteger.valueOf(benRegId));
		return dto;
	}

	@Test
	@DisplayName("full results are ordered by their mapping id")
	void fullResultsAreOrderedByMappingId() {
		List<BeneficiariesDTO> results = new ArrayList<>(
				Arrays.asList(beneficiary(30L), beneficiary(10L), beneficiary(20L)));

		Collections.sort(results);

		assertEquals(BigInteger.valueOf(10L), results.get(0).getBenMapId());
		assertEquals(BigInteger.valueOf(20L), results.get(1).getBenMapId());
		assertEquals(BigInteger.valueOf(30L), results.get(2).getBenMapId());
	}

	@Test
	@DisplayName("partial results are ordered by their registration id")
	void partialResultsAreOrderedByRegistrationId() {
		List<BeneficiariesPartialDTO> results = new ArrayList<>(
				Arrays.asList(partial(300L), partial(100L), partial(200L)));

		Collections.sort(results);

		assertEquals(BigInteger.valueOf(100L), results.get(0).getBenRegId());
		assertEquals(BigInteger.valueOf(300L), results.get(2).getBenRegId());
	}

	@Test
	@DisplayName("two results with the same key compare as equal")
	void resultsWithTheSameKeyCompareAsEqual() {
		assertEquals(0, beneficiary(10L).compareTo(beneficiary(10L)));
		assertEquals(0, partial(100L).compareTo(partial(100L)));
	}

	@Test
	@DisplayName("a result with no key cannot be ordered, which is why nulls are filtered first")
	void resultWithNoKeyCannotBeOrdered() {
		assertThrows(NullPointerException.class,
				() -> new BeneficiariesDTO().compareTo(beneficiary(10L)));
		assertThrows(NullPointerException.class,
				() -> new BeneficiariesPartialDTO().compareTo(partial(100L)));
	}
}
