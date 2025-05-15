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

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.domain.MBeneficiaryidentity;

@Repository
public interface BenIdentityRepo extends CrudRepository<MBeneficiaryidentity, BigInteger> {
	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.benIdentityId = :benIdentityId order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByBenIdentityId(@Param("benIdentityId") BigInteger benIdentityId);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.identityName = :identityName and beneficiary.identityNo = :identityNo "
			+ "order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByIdentityNameAndIdentityNo(@Param("identityName") String identityName,
			@Param("identityNo") String identityNo);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.identityType = :identityType and beneficiary.identityNo = :identityNo "
			+ "order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByIdentityTypeAndIdentityNo(@Param("identityType") String identityType,
			@Param("identityNo") String identityNo);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.identityName = :identityName and beneficiary.identityNo = :identityNo and "
			+ "beneficiary.identityType = :identityType order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByIdentityNameAndIdentityTypeAndIdentityNo(
			@Param("identityName") String identityName, @Param("identityType") String identityType,
			@Param("identityNo") String identityNo);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.identityName = :identityName and (beneficiary.identityNo = :identityNo or "
			+ "beneficiary.identityType = :identityType) order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByIdentityNameAndIdentityTypeOrIdentityNo(@Param("identityName") String identityName,
			@Param("identityType") String identityType, @Param("identityNo") String identityNo);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.createdDate >= :fromDate and beneficiary.createdDate <= :toDate order by "
			+ "beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByCreatedDateBetween(@Param("fromDate") Timestamp fromDate,
			@Param("toDate") Timestamp toDate);

	// by mapid and vanid
	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.benMapId = :benMapId AND beneficiary.vanID = :vanID  order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByBenMapIdAndVanID(@Param("benMapId") BigInteger benMapId,
			@Param("vanID") int vanID);

	@Query(value = "select beneficiary from MBeneficiaryidentity beneficiary where "
			+ "beneficiary.benMapId = :benMapId order by beneficiary.benMapId asc")
	List<MBeneficiaryidentity> findByBenMapId(@Param("benMapId") BigInteger benMapId);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiaryidentity set vanSerialNo = :benIdentityId WHERE benIdentityId = :benIdentityId")
	int updateVanSerialNo(@Param("benIdentityId") BigInteger benIdentityId);

	@Query(value = "select i from MBeneficiaryidentity i where i.identityNo = :identityNo")
	List<MBeneficiaryidentity> searchByIdentityNo(@Param("identityNo") String identityNo);

}
