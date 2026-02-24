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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

import org.springframework.beans.factory.annotation.Autowired;

import com.iemr.common.identity.domain.identity.Address;
import com.iemr.common.identity.domain.identity.MBeneficiaryaddress;
import com.iemr.common.identity.domain.identity.MBeneficiarycontact;
import com.iemr.common.identity.domain.identity.MBeneficiarydetail;
import com.iemr.common.identity.domain.identity.MBeneficiarymapping;
import com.iemr.common.identity.domain.identity.VBenAdvanceSearch;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.mapper.IdentitySearchMapper;

public class BenMappingRepoImpl implements BenMappingRepoCustom {

	@PersistenceContext
	private EntityManager entityManager;
	@Autowired
	IdentitySearchMapper searchMapper;
	@Autowired
	BenMappingRepo mappingRepo;

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
	public List<MBeneficiarymapping> dynamicFilterSearch(IdentitySearchDTO searchDTO) {

		CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
		CriteriaQuery<MBeneficiarymapping> criteriaQuery = criteriaBuilder.createQuery(MBeneficiarymapping.class);
		Root<MBeneficiarymapping> root = criteriaQuery.from(MBeneficiarymapping.class);
		// add predicates to see more parameters
		List<Predicate> predicates = new ArrayList<>();

		// for joining the tables whose column appears on where clause
		Join<MBeneficiarymapping, MBeneficiarydetail> benDetail = root.join("mBeneficiarydetail", JoinType.INNER);
		Join<MBeneficiarymapping, MBeneficiaryaddress> benAddress = root.join("mBeneficiaryaddress", JoinType.INNER);

		/**
		 * First Name is present, additional criteria needed
		 */
		if (searchDTO.getFirstName() != null) {

			predicates.add(criteriaBuilder.like(benDetail.get("firstName"), "%" + searchDTO.getFirstName() + "%"));
		}

		/**
		 * Middle Name is present, additional criteria needed
		 */
		if (searchDTO.getMiddleName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("middleName"), "%" + searchDTO.getMiddleName() + "%"));
		}

		/**
		 * Last Name is present, additional criteria needed
		 */
		if (searchDTO.getLastName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("lastName"), "%" + searchDTO.getLastName() + "%"));
		}

		/**
		 * Gender Id is present, additional criteria needed
		 */
		if (searchDTO.getGenderId() != null) {
			predicates.add(criteriaBuilder.equal(benDetail.get("genderId"), searchDTO.getGenderId()));
		}

		/**
		 * Gender Name is present, additional criteria needed
		 */
		if (searchDTO.getGenderName() != null) {
			predicates.add(criteriaBuilder.equal(benDetail.get("genderName"), searchDTO.getGenderName()));
		}

		/**
		 * Spouse Name is present, additional criteria needed
		 */
		if (searchDTO.getSpouseName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("spouseName"), "%" + searchDTO.getSpouseName() + "%"));
		}

		/**
		 * Father Name present, additional criteria needed
		 */
		if (searchDTO.getFatherName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("fatherName"), "%" + searchDTO.getFatherName() + "%"));
		}

		/**
		 * Pin Code is present, additional criteria needed
		 */
		if (searchDTO.getPinCode() != null) {
			predicates.add(criteriaBuilder.equal(benAddress.get("currPinCode"), searchDTO.getPinCode()));
		}

		/**
		 * District Id is present, additional criteria needed
		 */
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getDistrictId() != null) {
			predicates.add(criteriaBuilder.equal(benAddress.get("currDistrictId"),
					searchDTO.getCurrentAddress().getDistrictId()));
		}

		/**
		 * District Id is present, additional criteria needed
		 */
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getDistrict() != null) {
			predicates.add(
					criteriaBuilder.equal(benAddress.get("currDistrict"), searchDTO.getCurrentAddress().getDistrict()));
		}

		/**
		 * District Id is present, additional criteria needed
		 */
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getStateId() != null) {
			predicates.add(
					criteriaBuilder.equal(benAddress.get("currStateId"), searchDTO.getCurrentAddress().getStateId()));
		}

		/**
		 * District Id is present, additional criteria needed
		 */
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getState() != null) {
			predicates
					.add(criteriaBuilder.equal(benAddress.get("currState"), searchDTO.getCurrentAddress().getState()));
		}

		criteriaQuery.select(root).where(predicates.toArray(new Predicate[] {}))
				.orderBy(criteriaBuilder.asc(root.get("benMapId")));
		TypedQuery<MBeneficiarymapping> typedQuery = entityManager.createQuery(criteriaQuery);
		return typedQuery.getResultList();
	}

	/**
	 * IEMR generation for MCTS
	 */
	@Override
	public List<MBeneficiarymapping> finiteSearch(IdentityDTO identityDTO) {

		CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
		CriteriaQuery<MBeneficiarymapping> criteriaQuery = criteriaBuilder.createQuery(MBeneficiarymapping.class);
		Root<MBeneficiarymapping> root = criteriaQuery.from(MBeneficiarymapping.class);
		// add predicates to see more parameters
		List<Predicate> predicates = new ArrayList<>();

		// for joining the tables whose column appears on where clause
		Join<MBeneficiarymapping, MBeneficiarydetail> benDetail = root.join("mBeneficiarydetail", JoinType.INNER);
		Join<MBeneficiarymapping, MBeneficiaryaddress> benAddress = root.join("mBeneficiaryaddress", JoinType.INNER);
		Join<MBeneficiarymapping, MBeneficiarycontact> benContact = root.join("mBeneficiaryaddress", JoinType.INNER);

		/**
		 * First Name is present, additional criteria needed
		 */
		if (identityDTO.getFirstName() != null) {

			predicates.add(criteriaBuilder.like(benDetail.get("firstName"), "%" + identityDTO.getFirstName() + "%"));
		}

		/**
		 * Middle Name is present, additional criteria needed
		 */
		if (identityDTO.getMiddleName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("middleName"), "%" + identityDTO.getMiddleName() + "%"));
		}

		/**
		 * Last Name is present, additional criteria needed
		 */
		if (identityDTO.getLastName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("lastName"), "%" + identityDTO.getLastName() + "%"));
		}

		/**
		 * Gender Id is present, additional criteria needed
		 */
		if (identityDTO.getGenderId() != null) {
			predicates.add(criteriaBuilder.equal(benDetail.get("genderId"), identityDTO.getGenderId()));
		}

		/**
		 * Gender Name is present, additional criteria needed
		 */
		if (identityDTO.getGender() != null) {
			predicates.add(criteriaBuilder.equal(benDetail.get("genderName"), identityDTO.getGender()));
		}

		/**
		 * Spouse Name is present, additional criteria needed
		 */
		if (identityDTO.getSpouseName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("spouseName"), "%" + identityDTO.getSpouseName() + "%"));
		}

		/**
		 * community is present, additional criteria needed
		 */
		if (identityDTO.getCommunity() != null) {
			predicates.add(criteriaBuilder.equal(benDetail.get("community"), identityDTO.getCommunity()));
		}

		/**
		 * Father Name present, additional criteria needed
		 */
		if (identityDTO.getFatherName() != null) {
			predicates.add(criteriaBuilder.like(benDetail.get("fatherName"), "%" + identityDTO.getFatherName() + "%"));
		}

		// code if address is not empty

		if (identityDTO.getCurrentAddress() != null) {

			Address currAddress = identityDTO.getCurrentAddress();

			/**
			 * Pin Code is present, additional criteria needed
			 */
			if (currAddress.getPinCode() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currPinCode"), currAddress.getPinCode()));
			}

			/**
			 * District Id is present, additional criteria needed
			 */
			if (currAddress.getDistrictId() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currDistrictId"), currAddress.getDistrictId()));
			}

			/**
			 * District Id is present, additional criteria needed
			 */
			if (currAddress.getDistrict() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currDistrict"), currAddress.getDistrict()));
			}

			/**
			 * District Id is present, additional criteria needed
			 */
			if (currAddress.getStateId() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currStateId"), currAddress.getStateId()));
			}

			/**
			 * District Id is present, additional criteria needed
			 */
			if (currAddress.getState() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currState"), currAddress.getState()));
			}

			/**
			 * address line 1 is present, additional criteria needed
			 */
			if (currAddress.getAddrLine1() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currAddrLine1"), currAddress.getAddrLine1()));
			}

			/**
			 * currSubDistrictId is present, additional criteria needed
			 */
			if (currAddress.getSubDistrictId() != null) {
				predicates.add(
						criteriaBuilder.equal(benAddress.get("currSubDistrictId"), currAddress.getSubDistrictId()));
			}

			/**
			 * currSubDistrictId is present, additional criteria needed
			 */
			if (currAddress.getSubDistrict() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currSubDistrict"), currAddress.getSubDistrict()));
			}

			/**
			 * currVillageId is present, additional criteria needed
			 */
			if (currAddress.getVillageId() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currVillageId"), currAddress.getVillageId()));
			}

			/**
			 * currVillage is present, additional criteria needed
			 */
			if (currAddress.getVillage() != null) {
				predicates.add(criteriaBuilder.equal(benAddress.get("currVillage"), currAddress.getVillage()));
			}
		}

		if (identityDTO.getContact() != null && identityDTO.getContact().getPreferredPhoneNum() != null) {

			predicates.add(criteriaBuilder.equal(benContact.get("preferredPhoneNum"),
					identityDTO.getContact().getPreferredPhoneNum()));
		}

		criteriaQuery.select(root).where(predicates.toArray(new Predicate[] {}));
		TypedQuery<MBeneficiarymapping> typedQuery = entityManager.createQuery(criteriaQuery);
		return typedQuery.getResultList();
	}

	@Override
	public List<VBenAdvanceSearch> dynamicFilterSearchNew(IdentitySearchDTO searchDTO) {
		CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
		CriteriaQuery<VBenAdvanceSearch> criteriaQuery = criteriaBuilder.createQuery(VBenAdvanceSearch.class);
		Root<VBenAdvanceSearch> root = criteriaQuery.from(VBenAdvanceSearch.class);
		List<Predicate> predicateList = new ArrayList<>();

		// if firstName is not null
		if (searchDTO.getFirstName() != null) {
			predicateList.add(criteriaBuilder.like(root.get("firstName"), "%" + searchDTO.getFirstName() + "%"));
		}
		// if middleName is not null
		if (searchDTO.getMiddleName() != null) {
			predicateList.add(criteriaBuilder.like(root.get("middleName"), "%" + searchDTO.getMiddleName() + "%"));
		}
		// if lastName is not null
		if (searchDTO.getLastName() != null) {
			predicateList.add(criteriaBuilder.like(root.get("lastName"), "%" + searchDTO.getLastName() + "%"));
		}
		// if genderID is not null
		if (searchDTO.getGenderId() != null) {
			predicateList.add(criteriaBuilder.equal(root.get("genderID"), searchDTO.getGenderId()));
		}
		// if stateID is not null
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getStateId() != null) {
			predicateList.add(criteriaBuilder.equal(root.get("stateID"), searchDTO.getCurrentAddress().getStateId()));
		}
		// if districtID is not null
		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getDistrictId() != null) {
			predicateList
					.add(criteriaBuilder.equal(root.get("districtID"), searchDTO.getCurrentAddress().getDistrictId()));
		}

		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getSubDistrictId() != null) {
			predicateList.add(
					criteriaBuilder.equal(root.get("subDistrictID"), searchDTO.getCurrentAddress().getSubDistrictId()));
		}

		// if father Name is not null
		if (searchDTO.getFatherName() != null) {
			predicateList.add(criteriaBuilder.like(root.get("fatherName"), "%" + searchDTO.getFatherName() + "%"));
		}

		// if DOB is not null
		if (searchDTO.getDob() != null) {
			Date d = new Date(searchDTO.getDob().getTime());
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(d);
			calendar.add(Calendar.DATE, 1);
			Date beforeDate = calendar.getTime();
			Timestamp lastDate = new Timestamp(beforeDate.getTime());

			predicateList.add(criteriaBuilder.greaterThanOrEqualTo(root.get("dOB"), searchDTO.getDob()));
			predicateList.add(criteriaBuilder.lessThan(root.get("dOB"), lastDate));
//			criteriaBuilder.function("date", Date.class,root.get(root_.timestamp))
		}

		if (searchDTO.getHouseHoldID() != null)
			predicateList.add(criteriaBuilder.equal(root.get("houseHoldID"), searchDTO.getHouseHoldID()));

		if (searchDTO.getCurrentAddress() != null && searchDTO.getCurrentAddress().getVillageId() != null) {
			predicateList
					.add(criteriaBuilder.equal(root.get("villageID"), searchDTO.getCurrentAddress().getVillageId()));
		}

		criteriaQuery.select(root).where(predicateList.toArray(new Predicate[] {}));
		TypedQuery<VBenAdvanceSearch> typedQuery = entityManager.createQuery(criteriaQuery);
		return typedQuery.getResultList();
	}

}
