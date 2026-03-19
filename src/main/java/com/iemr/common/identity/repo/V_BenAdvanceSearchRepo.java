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
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.domain.VBenAdvanceSearch;

@Repository
public interface V_BenAdvanceSearchRepo extends CrudRepository<VBenAdvanceSearch, BigInteger> {

	@Query(nativeQuery = true, value = "SELECT BeneficiaryRegID, HealthID, HealthIDNumber, AuthenticationMode, CreatedDate"
			+ "  FROM db_iemr.m_benhealthidmapping WHERE BeneficiaryRegID=:benRegID")
	List<Object[]> getBenAbhaDetailsByBenRegID(@Param("benRegID") BigInteger benRegID);

	@Query(nativeQuery = true, value = "SELECT BeneficiaryRegID "
			+ "  FROM db_iemr.m_benhealthidmapping WHERE HealthID=:healthID AND BeneficiaryRegID is not null ")
	List<BigInteger> getBenRegIDByHealthIDAbhaAddress(@Param("healthID") String healthID);

	@Query(nativeQuery = true, value = "SELECT BeneficiaryRegID "
			+ "  FROM db_iemr.m_benhealthidmapping WHERE HealthIDNumber=:healthIDNo  AND BeneficiaryRegID is not null ")
	List<BigInteger> getBenRegIDByHealthIDNoAbhaIdNo(@Param("healthIDNo") String healthIDNo);

	//Batch fetch ABHA details for multiple beneficiaries
    @Query(nativeQuery = true, value = "SELECT BeneficiaryRegID, HealthID, HealthIDNumber, AuthenticationMode, CreatedDate"
            + " FROM db_iemr.m_benhealthidmapping WHERE BeneficiaryRegID IN :benRegIDs ORDER BY BeneficiaryRegID, CreatedDate DESC")
    List<Object[]> getBenAbhaDetailsByBenRegIDs(@Param("benRegIDs") List<BigInteger> benRegIDs);
    
}
