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
package com.iemr.common.identity.repo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.dto.IdentitySearchDTO;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;

/**
 * Tests for the advance-search query over the beneficiary-detail table.
 *
 * <p>
 * As with the mapping search, the risk is a supplied criterion that never
 * reaches the WHERE clause and quietly widens the result set, so the tests
 * assert the predicate count for each combination. The Criteria API is mocked,
 * so attribute names are not validated against the entity metamodel.
 */
class BenDetailRepoImplTest {

	private EntityManager entityManager;
	private CriteriaBuilder criteriaBuilder;
	private BenDetailRepoImpl repo;

	@BeforeEach
	void setUp() {
		entityManager = mock(EntityManager.class, RETURNS_DEEP_STUBS);
		criteriaBuilder = entityManager.getCriteriaBuilder();
		repo = new BenDetailRepoImpl();
		ReflectionTestUtils.setField(repo, "entityManager", entityManager);
	}

	@SuppressWarnings("unchecked")
	private Predicate[] capturedPredicates() {
		CriteriaQuery<MBeneficiarydetail> query = criteriaBuilder.createQuery(MBeneficiarydetail.class);
		ArgumentCaptor<Predicate[]> captor = ArgumentCaptor.forClass(Predicate[].class);
		verify(query.select(any())).where(captor.capture());
		return captor.getValue();
	}

	@Test
	@DisplayName("a search with no criteria produces no predicates")
	void searchWithNoCriteriaProducesNoPredicates() {
		repo.advanceFilterSearch(new IdentitySearchDTO());

		assertEquals(0, capturedPredicates().length);
	}

	@Test
	@DisplayName("each name criterion contributes one predicate")
	void eachNameCriterionContributesOnePredicate() {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setFirstName("Asha");
		searchDTO.setMiddleName("Rani");
		searchDTO.setLastName("Devi");
		searchDTO.setSpouseName("Suresh");
		searchDTO.setFatherName("Ram");

		repo.advanceFilterSearch(searchDTO);

		assertEquals(5, capturedPredicates().length);
	}

	@Test
	@DisplayName("each age and gender criterion contributes one predicate")
	void eachAgeAndGenderCriterionContributesOnePredicate() {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setAgeId(3);
		searchDTO.setAge(30);
		searchDTO.setGenderId(2);
		searchDTO.setGenderName("Female");

		repo.advanceFilterSearch(searchDTO);

		assertEquals(4, capturedPredicates().length);
	}

	@Test
	@DisplayName("a pin code contributes one predicate")
	void pinCodeContributesOnePredicate() {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setPinCode("560064");

		repo.advanceFilterSearch(searchDTO);

		assertEquals(1, capturedPredicates().length);
	}

	@Test
	@DisplayName("every criterion together produces one predicate each")
	void everyCriterionTogetherProducesOnePredicateEach() {
		IdentitySearchDTO searchDTO = new IdentitySearchDTO();
		searchDTO.setFirstName("Asha");
		searchDTO.setMiddleName("Rani");
		searchDTO.setLastName("Devi");
		searchDTO.setAgeId(3);
		searchDTO.setAge(30);
		searchDTO.setGenderId(2);
		searchDTO.setGenderName("Female");
		searchDTO.setSpouseName("Suresh");
		searchDTO.setFatherName("Ram");
		searchDTO.setPinCode("560064");

		repo.advanceFilterSearch(searchDTO);

		assertEquals(10, capturedPredicates().length);
	}

	@Test
	@DisplayName("the matching rows are returned from the executed query")
	void matchingRowsAreReturned() {
		List<MBeneficiarydetail> rows = Collections.singletonList(new MBeneficiarydetail());
		CriteriaQuery<MBeneficiarydetail> query = criteriaBuilder.createQuery(MBeneficiarydetail.class);
		@SuppressWarnings("unchecked")
		TypedQuery<MBeneficiarydetail> typedQuery = mock(TypedQuery.class);
		when(entityManager.createQuery(query)).thenReturn(typedQuery);
		when(typedQuery.getResultList()).thenReturn(rows);

		assertSame(rows, repo.advanceFilterSearch(new IdentitySearchDTO()));
	}
}
