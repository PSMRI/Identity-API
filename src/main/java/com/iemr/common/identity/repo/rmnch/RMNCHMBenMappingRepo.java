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
package com.iemr.common.identity.repo.rmnch;

import java.math.BigInteger;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarymapping;

@Repository
public interface RMNCHMBenMappingRepo extends CrudRepository<RMNCHMBeneficiarymapping, BigInteger> {
	@Query(" SELECT t FROM RMNCHMBeneficiarymapping t WHERE t.id = :vanSerialNo AND t.VanID = :vanID")
	public RMNCHMBeneficiarymapping getByIdAndVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") int vanID);

	@Query(" SELECT t FROM RMNCHMBeneficiarymapping t WHERE t.benAddressId = :addressID AND t.VanID = :vanID")
	public RMNCHMBeneficiarymapping getByAddressIDAndVanID(@Param("addressID") BigInteger addressID,
			@Param("vanID") int vanID);

	@Query(nativeQuery = true, value = " SELECT userid FROM db_iemr.m_user WHERE UserName = :userName ")
	public Integer getUserIDByUserName(@Param("userName") String userName);

	@Query(" SELECT t FROM RMNCHMBeneficiarymapping t WHERE t.benRegId = :benRegId")
	public RMNCHMBeneficiarymapping getByBenRegId(@Param("benRegId") BigInteger benRegId);

}
