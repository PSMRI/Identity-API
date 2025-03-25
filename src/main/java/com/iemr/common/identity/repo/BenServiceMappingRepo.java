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

import com.iemr.common.identity.domain.MBeneficiaryservicemapping;

@Repository
public interface BenServiceMappingRepo extends CrudRepository<MBeneficiaryservicemapping, BigInteger> {
	List<MBeneficiaryservicemapping> findByBenServiceMapID(BigInteger benServiceMapID);

	List<MBeneficiaryservicemapping> findByProviderServiceMapId(Integer registeredProviderServiceMapId);

	List<MBeneficiaryservicemapping> findByProviderServiceMapIdAndRegisteredDateBetween(
			Integer registeredProviderServiceMapId, Timestamp from, Timestamp to);

	List<MBeneficiaryservicemapping> findByServiceIdAndRegisteredDateBetween(Integer serviceId, Timestamp from,
			Timestamp to);

	List<MBeneficiaryservicemapping> findByServiceNameAndRegisteredDateBetween(String serviceName, Timestamp from,
			Timestamp to);

	List<MBeneficiaryservicemapping> findByStateIdAndRegisteredDateBetween(Integer stateId, Timestamp from,
			Timestamp to);

	List<MBeneficiaryservicemapping> findByStateNameAndRegisteredDateBetween(String stateName, Timestamp from,
			Timestamp to);

	List<MBeneficiaryservicemapping> findByServiceProviderIdAndRegisteredDateBetween(Integer serviceProviderId,
			Timestamp from, Timestamp to);

	List<MBeneficiaryservicemapping> findByServiceProviderNameAndRegisteredDateBetween(String serviceProviderName,
			Timestamp from, Timestamp to);

	Long countByProviderServiceMapIdAndRegisteredDateBetween(Integer registeredProviderServiceMapId, Timestamp from,
			Timestamp to);

	Long countByServiceIdAndRegisteredDateBetween(Integer serviceId, Timestamp from, Timestamp to);

	Long countByServiceNameAndRegisteredDateBetween(String serviceName, Timestamp from, Timestamp to);

	Long countByStateIdAndRegisteredDateBetween(Integer stateId, Timestamp from, Timestamp to);

	Long countByStateNameAndRegisteredDateBetween(String stateName, Timestamp from, Timestamp to);

	Long countByServiceProviderIdAndRegisteredDateBetween(Integer serviceProviderId, Timestamp from, Timestamp to);

	Long countByServiceProviderNameAndRegisteredDateBetween(String serviceProviderName, Timestamp from, Timestamp to);

	List<MBeneficiaryservicemapping> findByCreatedDateBetween(Timestamp fromDate, Timestamp toDate);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiaryservicemapping set vanSerialNo = :benServiceMapID WHERE benServiceMapID = :benServiceMapID ")
	int updateVanSerialNo(@Param("benServiceMapID") BigInteger benServiceMapID);
	
	@Query("SELECT a FROM MBeneficiaryservicemapping a WHERE a.benMapId =:benMapId AND a.vanID =:vanID ")
	List<MBeneficiaryservicemapping> getWithVanSerialNoVanID(@Param("benMapId") BigInteger benMapId,
			@Param("vanID") Integer vanID);
}
