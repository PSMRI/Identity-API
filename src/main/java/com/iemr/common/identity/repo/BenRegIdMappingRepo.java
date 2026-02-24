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

import com.iemr.common.identity.domain.identity.MBeneficiaryregidmapping;

import jakarta.transaction.Transactional;

@Repository
public interface BenRegIdMappingRepo extends CrudRepository<MBeneficiaryregidmapping, BigInteger> {
	List<MBeneficiaryregidmapping> findByBenRegIdOrderByBenRegIdAsc(BigInteger benRegId);
	@Query("SELECT a FROM MBeneficiaryregidmapping a WHERE a.beneficiaryID =:beneficiaryID")
	MBeneficiaryregidmapping findByBeneficiaryID(@Param("beneficiaryID") BigInteger beneficiaryID);

	List<MBeneficiaryregidmapping> findByCreatedDateBetweenOrderByBenRegIdAsc(Timestamp fromDate, Timestamp toDate);

	MBeneficiaryregidmapping findFirstByProvisionedAndReserved(Boolean isProvisioned, Boolean isReserved);

	List<MBeneficiaryregidmapping> findByProviderServiceMapIDOrderByBenRegIdAsc(Integer providerServiceMapID);

	List<MBeneficiaryregidmapping> findByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(
			Integer providerServiceMapID, String vehicalNo);

	MBeneficiaryregidmapping findFirstByProvisionedAndVehicalNoOrderByBenRegIdAsc(Boolean isProvisioned,
			String vehicalNo);

	MBeneficiaryregidmapping findFirstByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(Integer providerServiceMapID,
			String vehicalNo);

	Long countByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(Integer providerServiceMapID, String vehicalNo);

	Long countByProvisioned(Boolean isProvisioned);

	@Transactional
	@Modifying
	@Query("update MBeneficiaryregidmapping set providerServiceMapID = null , vehicalNo = null "
			+ "where providerServiceMapID = :providerServiceMapID and vehicalNo = :vehicalNo")
	public int unreserveBeneficiaryIds(@Param("providerServiceMapID") Integer providerServiceMapID,
			@Param("vehicalNo") String vehicalNo);

	@Query("SELECT a FROM MBeneficiaryregidmapping a WHERE a.benRegId =:vanSerialNo AND a.vanID =:vanID ")
	MBeneficiaryregidmapping getWithVanSerialNoVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);
	


	List<MBeneficiaryregidmapping> findTop10000ByProvisionedAndReserved(Boolean isProvisioned,Boolean isReserved);

}
