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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.iemr.common.identity.repo.BenMappingRepo;

/**
 * Tests for the transaction boundary the re-index paging runs inside.
 *
 * <p>
 * Each page runs in its own short transaction so a rebuild does not hold one
 * connection open for the whole run. That makes this class thin, but it is the
 * one place where a query failure has to propagate rather than be swallowed:
 * the callers count on an exception to trigger their retry and backoff.
 */
@ExtendWith(MockitoExtension.class)
class BeneficiaryTransactionHelperTest {

	@Mock
	private BenMappingRepo mappingRepo;

	@InjectMocks
	private BeneficiaryTransactionHelper helper;

	@Test
	@DisplayName("a page of beneficiary ids is fetched with the requested window")
	void pageOfIdsIsFetchedWithTheRequestedWindow() {
		List<Object[]> page = Collections.singletonList(new Object[] { BigInteger.ONE });
		when(mappingRepo.getBeneficiaryIdsBatch(2000, 500)).thenReturn(page);

		assertEquals(page, helper.getBeneficiaryIdsBatch(2000, 500));
	}

	@Test
	@DisplayName("a failing page query propagates so the caller can retry it")
	void failingPageQueryPropagates() {
		when(mappingRepo.getBeneficiaryIdsBatch(0, 500)).thenThrow(new IllegalStateException("connection reset"));

		assertThrows(IllegalStateException.class, () -> helper.getBeneficiaryIdsBatch(0, 500));
	}

	@Test
	@DisplayName("the active beneficiary count is passed through")
	void activeCountIsPassedThrough() {
		when(mappingRepo.countActiveBeneficiaries()).thenReturn(784_000L);

		assertEquals(784_000L, helper.countActiveBeneficiaries());
	}

	@Test
	@DisplayName("a failing count propagates rather than reporting zero")
	void failingCountPropagates() {
		// Zero would make the caller skip the whole rebuild.
		when(mappingRepo.countActiveBeneficiaries()).thenThrow(new IllegalStateException("table locked"));

		assertThrows(IllegalStateException.class, () -> helper.countActiveBeneficiaries());
	}

	@Test
	@DisplayName("a beneficiary with at least one active row exists")
	void beneficiaryWithAnActiveRowExists() {
		when(mappingRepo.countActiveByBenRegId(BigInteger.TEN)).thenReturn(1L);

		assertTrue(helper.existsByBenRegId(BigInteger.TEN));
	}

	@Test
	@DisplayName("a beneficiary with no active row does not exist")
	void beneficiaryWithNoActiveRowDoesNotExist() {
		when(mappingRepo.countActiveByBenRegId(BigInteger.TEN)).thenReturn(0L);

		assertFalse(helper.existsByBenRegId(BigInteger.TEN));
	}

	@Test
	@DisplayName("a failing existence check propagates rather than reporting absence")
	void failingExistenceCheckPropagates() {
		when(mappingRepo.countActiveByBenRegId(BigInteger.TEN))
				.thenThrow(new IllegalStateException("connection reset"));

		assertThrows(IllegalStateException.class, () -> helper.existsByBenRegId(BigInteger.TEN));
	}
}
