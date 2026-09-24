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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.iemr.common.identity.domain.Address;
import com.iemr.common.identity.domain.Contact;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.domain.VBenAdvanceSearch;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

/**
 * Tests for the dynamic beneficiary-search queries.
 *
 * <p>
 * These build a Criteria query one optional predicate at a time. The failure
 * mode is not a crash but a missing predicate: a criterion the caller supplied
 * that never makes it into the WHERE clause silently widens the search, which
 * on this dataset means returning other people's records to a call-centre
 * agent. So the tests assert the <em>number</em> of predicates each combination
 * of criteria produces, and that the joins the WHERE clause depends on are
 * added.
 *
 * <p>
 * The Criteria API is driven through mocks, so these tests do not verify that
 * the attribute names passed to {@code get(...)} exist on the entities - only
 * an integration test against a real metamodel can do that.
 */
class BenMappingRepoImplTest {

	private EntityManager entityManager;
	private CriteriaBuilder criteriaBuilder;
	private BenMappingRepoImpl repo;

	@BeforeEach
	void setUp() {
		entityManager = mock(EntityManager.class, RETURNS_DEEP_STUBS);
		criteriaBuilder = entityManager.getCriteriaBuilder();
		repo = new BenMappingRepoImpl();
		ReflectionTestUtils.setField(repo, "entityManager", entityManager);
	}

	/** Captures the predicates handed to the WHERE clause of a mapping query. */
	@SuppressWarnings("unchecked")
	private Predicate[] mappingPredicates() {
		CriteriaQuery<MBeneficiarymapping> query = criteriaBuilder.createQuery(MBeneficiarymapping.class);
		ArgumentCaptor<Predicate[]> captor = ArgumentCaptor.forClass(Predicate[].class);
		verify(query.select(any())).where(captor.capture());
		return captor.getValue();
	}

	/** Captures the predicates handed to the WHERE clause of an advance-search query. */
	@SuppressWarnings("unchecked")
	private Predicate[] advanceSearchPredicates() {
		CriteriaQuery<VBenAdvanceSearch> query = criteriaBuilder.createQuery(VBenAdvanceSearch.class);
		ArgumentCaptor<Predicate[]> captor = ArgumentCaptor.forClass(Predicate[].class);
		verify(query.select(any())).where(captor.capture());
		return captor.getValue();
	}

	private Address address() {
		Address address = new Address();
		address.setPinCode("560064");
		address.setStateId(101);
		address.setState("Karnataka");
		address.setDistrictId(201);
		address.setDistrict("Bengaluru");
		address.setSubDistrictId(301);
		address.setSubDistrict("North");
		address.setVillageId(401);
		address.setVillage("Yelahanka");
		address.setAddrLine1("1 Main Rd");
		return address;
	}

	@Nested
	@DisplayName("advance search over the search view")
	class AdvanceSearch {

		@Test
		@DisplayName("a search with no criteria produces no predicates")
		void searchWithNoCriteriaProducesNoPredicates() {
			repo.dynamicFilterSearchNew(new IdentitySearchDTO());

			assertEquals(0, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("each supplied name and gender criterion contributes one predicate")
		void eachNameAndGenderCriterionContributesOnePredicate() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setFirstName("Asha");
			searchDTO.setMiddleName("Rani");
			searchDTO.setLastName("Devi");
			searchDTO.setGenderId(2);
			searchDTO.setFatherName("Ram");

			repo.dynamicFilterSearchNew(searchDTO);

			assertEquals(5, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("each address level supplied contributes one predicate")
		void eachAddressLevelContributesOnePredicate() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setCurrentAddress(address());

			repo.dynamicFilterSearchNew(searchDTO);

			// state, district, sub-district and village
			assertEquals(4, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("an address with no location set contributes no predicates")
		void addressWithNoLocationContributesNoPredicates() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setCurrentAddress(new Address());

			repo.dynamicFilterSearchNew(searchDTO);

			assertEquals(0, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("a date of birth is matched as a whole day, not an instant")
		void dateOfBirthIsMatchedAsAWholeDay() {
			// The column is a timestamp, so an equality match would only find
			// beneficiaries recorded at exactly midnight.
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setDob(Timestamp.valueOf("1996-01-15 00:00:00"));

			repo.dynamicFilterSearchNew(searchDTO);

			assertEquals(2, advanceSearchPredicates().length);
			verify(criteriaBuilder).greaterThanOrEqualTo(any(), any(Timestamp.class));
			ArgumentCaptor<Timestamp> upperBound = ArgumentCaptor.forClass(Timestamp.class);
			verify(criteriaBuilder).lessThan(any(), upperBound.capture());
			assertEquals(Timestamp.valueOf("1996-01-16 00:00:00"), upperBound.getValue());
		}

		@Test
		@DisplayName("a household id contributes one predicate")
		void householdIdContributesOnePredicate() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setHouseHoldID(555L);

			repo.dynamicFilterSearchNew(searchDTO);

			assertEquals(1, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("every criterion together produces one predicate each")
		void everyCriterionTogetherProducesOnePredicateEach() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setFirstName("Asha");
			searchDTO.setMiddleName("Rani");
			searchDTO.setLastName("Devi");
			searchDTO.setGenderId(2);
			searchDTO.setFatherName("Ram");
			searchDTO.setDob(Timestamp.valueOf("1996-01-15 00:00:00"));
			searchDTO.setHouseHoldID(555L);
			searchDTO.setCurrentAddress(address());

			repo.dynamicFilterSearchNew(searchDTO);

			// 5 name/gender/father + 4 address levels + 2 date bounds + household
			assertEquals(12, advanceSearchPredicates().length);
		}

		@Test
		@DisplayName("the matching rows are returned from the executed query")
		void matchingRowsAreReturned() {
			List<VBenAdvanceSearch> rows = Collections.singletonList(new VBenAdvanceSearch());
			CriteriaQuery<VBenAdvanceSearch> query = criteriaBuilder.createQuery(VBenAdvanceSearch.class);
			TypedQuery<VBenAdvanceSearch> typedQuery = mock(TypedQuery.class);
			when(entityManager.createQuery(query)).thenReturn(typedQuery);
			when(typedQuery.getResultList()).thenReturn(rows);

			assertSame(rows, repo.dynamicFilterSearchNew(new IdentitySearchDTO()));
		}
	}

	@Nested
	@DisplayName("dynamic filter search over the mapping table")
	class DynamicFilterSearch {

		@Test
		@DisplayName("the detail and address tables are joined so their columns can be filtered")
		void detailAndAddressTablesAreJoined() {
			CriteriaQuery<MBeneficiarymapping> query = criteriaBuilder.createQuery(MBeneficiarymapping.class);
			Root<MBeneficiarymapping> root = query.from(MBeneficiarymapping.class);

			repo.dynamicFilterSearch(new IdentitySearchDTO());

			verify(root).join("mBeneficiarydetail", JoinType.INNER);
			verify(root).join("mBeneficiaryaddress", JoinType.INNER);
		}

		@Test
		@DisplayName("a search with no criteria produces no predicates")
		void searchWithNoCriteriaProducesNoPredicates() {
			repo.dynamicFilterSearch(new IdentitySearchDTO());

			assertEquals(0, mappingPredicates().length);
		}

		@Test
		@DisplayName("each beneficiary-detail criterion contributes one predicate")
		void eachDetailCriterionContributesOnePredicate() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setFirstName("Asha");
			searchDTO.setMiddleName("Rani");
			searchDTO.setLastName("Devi");
			searchDTO.setGenderId(2);
			searchDTO.setGenderName("Female");
			searchDTO.setSpouseName("Suresh");
			searchDTO.setFatherName("Ram");

			repo.dynamicFilterSearch(searchDTO);

			assertEquals(7, mappingPredicates().length);
		}

		@Test
		@DisplayName("each address criterion contributes one predicate")
		void eachAddressCriterionContributesOnePredicate() {
			IdentitySearchDTO searchDTO = new IdentitySearchDTO();
			searchDTO.setPinCode("560064");
			searchDTO.setCurrentAddress(address());

			repo.dynamicFilterSearch(searchDTO);

			// pin code plus district id, district, state id and state
			assertEquals(5, mappingPredicates().length);
		}

		@Test
		@DisplayName("results are ordered by mapping id so paging is stable")
		void resultsAreOrderedByMappingId() {
			CriteriaQuery<MBeneficiarymapping> query = criteriaBuilder.createQuery(MBeneficiarymapping.class);

			repo.dynamicFilterSearch(new IdentitySearchDTO());

			verify(query.select(any()).where(new Predicate[] {})).orderBy(any(jakarta.persistence.criteria.Order.class));
		}

		@Test
		@DisplayName("the matching rows are returned from the executed query")
		void matchingRowsAreReturned() {
			List<MBeneficiarymapping> rows = Collections.singletonList(new MBeneficiarymapping());
			CriteriaQuery<MBeneficiarymapping> query = criteriaBuilder.createQuery(MBeneficiarymapping.class);
			TypedQuery<MBeneficiarymapping> typedQuery = mock(TypedQuery.class);
			when(entityManager.createQuery(query)).thenReturn(typedQuery);
			when(typedQuery.getResultList()).thenReturn(rows);

			assertSame(rows, repo.dynamicFilterSearch(new IdentitySearchDTO()));
		}
	}

	@Nested
	@DisplayName("finite search for identifier generation")
	class FiniteSearch {

		@Test
		@DisplayName("a search with no criteria produces no predicates")
		void searchWithNoCriteriaProducesNoPredicates() {
			repo.finiteSearch(new IdentityDTO());

			assertEquals(0, mappingPredicates().length);
		}

		@Test
		@DisplayName("each beneficiary-detail criterion contributes one predicate")
		void eachDetailCriterionContributesOnePredicate() {
			IdentityDTO identityDTO = new IdentityDTO();
			identityDTO.setFirstName("Asha");
			identityDTO.setMiddleName("Rani");
			identityDTO.setLastName("Devi");
			identityDTO.setGenderId(2);
			identityDTO.setGender("Female");
			identityDTO.setSpouseName("Suresh");
			identityDTO.setFatherName("Ram");
			identityDTO.setCommunity("General");

			repo.finiteSearch(identityDTO);

			assertEquals(8, mappingPredicates().length);
		}

		@Test
		@DisplayName("every level of the current address contributes one predicate")
		void everyAddressLevelContributesOnePredicate() {
			IdentityDTO identityDTO = new IdentityDTO();
			identityDTO.setCurrentAddress(address());

			repo.finiteSearch(identityDTO);

			// pin code, district id, district, state id, state, address line 1,
			// sub-district id, sub-district, village id and village
			assertEquals(10, mappingPredicates().length);
		}

		@Test
		@DisplayName("an absent address contributes no predicates")
		void absentAddressContributesNoPredicates() {
			IdentityDTO identityDTO = new IdentityDTO();
			identityDTO.setFirstName("Asha");

			repo.finiteSearch(identityDTO);

			assertEquals(1, mappingPredicates().length);
		}

		@Test
		@DisplayName("a preferred phone number contributes one predicate")
		void preferredPhoneNumberContributesOnePredicate() {
			IdentityDTO identityDTO = new IdentityDTO();
			Contact contact = new Contact();
			contact.setPreferredPhoneNum("9000000000");
			identityDTO.setContact(contact);

			repo.finiteSearch(identityDTO);

			assertEquals(1, mappingPredicates().length);
		}

		@Test
		@DisplayName("a contact with no preferred number contributes no predicates")
		void contactWithNoPreferredNumberContributesNoPredicates() {
			IdentityDTO identityDTO = new IdentityDTO();
			identityDTO.setContact(new Contact());

			repo.finiteSearch(identityDTO);

			assertEquals(0, mappingPredicates().length);
		}

		@Test
		@DisplayName("the matching rows are returned from the executed query")
		void matchingRowsAreReturned() {
			List<MBeneficiarymapping> rows = Collections.singletonList(new MBeneficiarymapping());
			CriteriaQuery<MBeneficiarymapping> query = criteriaBuilder.createQuery(MBeneficiarymapping.class);
			TypedQuery<MBeneficiarymapping> typedQuery = mock(TypedQuery.class);
			when(entityManager.createQuery(query)).thenReturn(typedQuery);
			when(typedQuery.getResultList()).thenReturn(rows);

			assertSame(rows, repo.finiteSearch(new IdentityDTO()));
		}
	}
}
