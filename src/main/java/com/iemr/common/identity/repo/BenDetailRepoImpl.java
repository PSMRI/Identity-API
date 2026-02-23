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

import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.springframework.beans.factory.annotation.Autowired;

import com.iemr.common.identity.domain.identity.MBeneficiarydetail;
import com.iemr.common.identity.dto.IdentitySearchDTO;

public class BenDetailRepoImpl implements BenDetailRepoCustom {

	@PersistenceContext
	private EntityManager entityManager;
	@Autowired
	BenRegIdMappingRepo regIdRepo;

	/**
	 * Search Criteria: the following are necessary to conduct a finite search
	 * 
	 * 1. beneficiary ID 2. beneficiary Reg ID 3. Contact Number 4. Name (First,
	 * Middle, Last), Age, Gender 5. Village Name 6. District State Zone Coordinates
	 * 7. Pin Code 8. Father Name 9. Spouse Name
	 * 
	 * Allowed Single Parameters: - beneficiary Id - beneficiary Reg Id - Contact
	 * Number
	 * 
	 * Allowed Multi-Parameters: - Name (First, Middle, Last), Age, Gender - Contact
	 * Number & Name (First, Middle, Last) - Pin Code & Name (First, Middle, Last) -
	 * Village, Father Name & Name (First, Middle, Last) - Village, Spouse Name &
	 * Name (First, Middle, Last) - Village, District, State - Govt identities(keys,
	 * values)
	 */

	@Override
	public List<MBeneficiarydetail> advanceFilterSearch(IdentitySearchDTO searchDTO) {

		CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
		CriteriaQuery<MBeneficiarydetail> criteriaQuery = criteriaBuilder.createQuery(MBeneficiarydetail.class);
		Root<MBeneficiarydetail> root = criteriaQuery.from(MBeneficiarydetail.class);

		// add predicates to see more parameters
		List<Predicate> predicates = new ArrayList<Predicate>();

		/**
		 * First Name is present, additional criteria needed
		 */
		if (searchDTO.getFirstName() != null) {

			predicates.add(criteriaBuilder.like(root.<String>get("firstName"), "%" + searchDTO.getFirstName() + "%"));
		}

		/**
		 * Middle Name is present, additional criteria needed
		 */
		if (searchDTO.getMiddleName() != null) {
			predicates.add(criteriaBuilder.like(root.<String>get("middleName"), "%" + searchDTO.getMiddleName() + "%"));
		}

		/**
		 * Last Name is present, additional criteria needed
		 */
		if (searchDTO.getLastName() != null) {
			predicates.add(criteriaBuilder.like(root.<String>get("lastName"), "%" + searchDTO.getLastName() + "%"));
		}

		/**
		 * Age Id is present, additional criteria needed
		 */
		if (searchDTO.getAgeId() != null) {
			predicates.add(criteriaBuilder.equal(root.get("ageId"), searchDTO.getAgeId()));
		}

		/**
		 * Age is present, additional criteria needed
		 */
		if (searchDTO.getAge() != null) {
			predicates.add(criteriaBuilder.equal(root.get("age"), searchDTO.getAge()));
		}

		/**
		 * Gender Id is present, additional criteria needed
		 */
		if (searchDTO.getGenderId() != null) {
			predicates.add(criteriaBuilder.equal(root.get("genderId"), searchDTO.getGenderId()));
		}

		/**
		 * Gender Name is present, additional criteria needed
		 */
		if (searchDTO.getGenderName() != null) {
			predicates.add(criteriaBuilder.equal(root.get("genderName"), searchDTO.getGenderName()));
		}

		/**
		 * Spouse Name is present, additional criteria needed
		 */
		if (searchDTO.getSpouseName() != null) {
			predicates.add(criteriaBuilder.like(root.<String>get("spouseName"), "%" + searchDTO.getSpouseName() + "%"));
		}

		/**
		 * Father Name present, additional criteria needed
		 */
		if (searchDTO.getFatherName() != null) {
			predicates.add(criteriaBuilder.like(root.<String>get("fatherName"), "%" + searchDTO.getFatherName() + "%"));
		}

		/**
		 * Pin Code is present, additional criteria needed
		 */
		if (searchDTO.getPinCode() != null) {
			predicates.add(criteriaBuilder.equal(root.get("pinCode"), searchDTO.getPinCode()));
		}

		/**
		 * Contact Number is present, additional criteria needed
		 */

		criteriaQuery.select(root).where(predicates.toArray(new Predicate[] {}))
				.orderBy(criteriaBuilder.asc(root.get("benMapId")));
		TypedQuery<MBeneficiarydetail> typedQuery = entityManager.createQuery(criteriaQuery);
		return typedQuery.getResultList();
	}

}
