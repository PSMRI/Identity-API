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

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryregidmapping;

@Repository
public interface RMNCHMBenRegIdMapRepo extends CrudRepository<RMNCHMBeneficiaryregidmapping, Long> {
	@Query(" SELECT beneficiaryID FROM RMNCHMBeneficiaryregidmapping t "
			+ " WHERE t.benRegId = :benRegID AND t.vanID = :vanID ")
	public BigInteger getBenIdFromRegIDAndVanID(@Param("benRegID") Long benRegID, @Param("vanID") int vanID);

	@Query(" SELECT beneficiaryID FROM RMNCHMBeneficiaryregidmapping t  WHERE t.benRegId = :benRegID ")
	public BigInteger getBenIdFromRegID(@Param("benRegID") Long benRegID);

	@Transactional
	@Modifying
	@Query(" UPDATE RMNCHMBeneficiaryregidmapping t set t.providerServiceMapID = :providerServiceMapID "
			+ " WHERE t.benRegId = :benRegID ")
	public int updateProviderServiceMapID(@Param("benRegID") BigInteger benRegID,
			@Param("providerServiceMapID") int providerServiceMapID);

	@Query(" SELECT t.benRegId FROM RMNCHMBeneficiaryregidmapping t  WHERE t.beneficiaryID = :benID ")
	public Long getRegID(@Param("benID") Long benID);
}
