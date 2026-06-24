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
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.data.rmnch.RMNCHCBACdetails;

@Repository
public interface RMNCHCBACDetailsRepo extends CrudRepository<RMNCHCBACdetails, Long> {
	@Query(" SELECT t FROM RMNCHCBACdetails t WHERE t.id = :vanSerialNo AND t.VanID = :vanID")
	public RMNCHCBACdetails getByIdAndVanID(@Param("vanSerialNo") Long vanSerialNo, @Param("vanID") int vanID);

	@Query(" SELECT t FROM RMNCHCBACdetails t WHERE t.BenRegId =:benRegID")
	public List<RMNCHCBACdetails> getByRegID(@Param("benRegID") BigInteger benRegID);

	@Query(value = "select beneficiary_visit_code,visit_category from db_iemr.i_ben_flow_outreach where beneficiary_reg_id=:benRegID AND beneficiary_visit_code is not null AND visit_category is not null order by created_date desc limit 1", nativeQuery = true)
	public List<Object[]> getVisitDetailsbyRegID(@Param("benRegID") Long benRegID);

	@Query(value = "select DiagnosisProvided from db_iemr.t_prescription where BeneficiaryRegID=:benRegID AND visitcode=:visitCode AND DiagnosisProvided is not null AND DiagnosisProvided <> '' ", nativeQuery = true)
	public List<Object> getDiagnosisProvidedCommon(@Param("benRegID") Long benRegID,
			@Param("visitCode") Long visitCode);

	// Add snomed code
	@Query(value = "select ProvisionalDiagnosis from db_iemr.t_PNCDiagnosis where BeneficiaryRegID=:benRegID AND visitcode=:visitCode AND ProvisionalDiagnosis is not null AND ProvisionalDiagnosis <> '' ", nativeQuery = true)
	public List<Object> getDiagnosisProvidedPNC(@Param("benRegID") Long benRegID, @Param("visitCode") Long visitCode);
	
	@Query(value = "select NCD_Condition from db_iemr.t_ncddiagnosis where BeneficiaryRegID=:benRegID AND visitcode=:visitCode AND NCD_Condition is not null AND NCD_Condition <> '' "
	, nativeQuery = true)
	public List<Object> getDiagnosisProvidedNCDCare(@Param("benRegID") Long benRegID, @Param("visitCode") Long visitCode);
	
}
