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

import com.iemr.common.identity.domain.MBeneficiaryfamilymapping;

@Repository
public interface BenFamilyMappingRepo extends CrudRepository<MBeneficiaryfamilymapping, BigInteger> {
	List<MBeneficiaryfamilymapping> findByBenFamilyMapIdOrderByBenFamilyMapIdAsc(BigInteger benFamilyMapId);

	List<MBeneficiaryfamilymapping> findByCreatedDateBetweenOrderByBenFamilyMapIdAsc(Timestamp fromDate,
			Timestamp toDate);


	List<MBeneficiaryfamilymapping> findByBenMapIdOrderByBenFamilyMapIdAsc(BigInteger benMapId);

	List<MBeneficiaryfamilymapping> findByBenMapIdAndVanIDOrderByBenFamilyMapIdAsc(BigInteger benMapId, int vanID);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiaryfamilymapping set vanSerialNo = :benFamilyMapId WHERE benFamilyMapId = :benFamilyMapId")
	int updateVanSerialNo(@Param("benFamilyMapId") BigInteger benFamilyMapId);
	

}
