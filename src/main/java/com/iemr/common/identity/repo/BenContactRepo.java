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

import com.iemr.common.identity.domain.identity.MBeneficiarycontact;

@Repository
public interface BenContactRepo extends CrudRepository<MBeneficiarycontact, BigInteger> {
	List<MBeneficiarycontact> findByBenContactsIDOrderByBenContactsIDAsc(BigInteger benContactsID);

	List<MBeneficiarycontact> findByCreatedDateBetweenOrderByBenContactsIDAsc(Timestamp fromDate, Timestamp toDate);

	List<MBeneficiarycontact> findByEmergencyContactNumOrderByBenContactsIDAsc(String emergencyContactNum);

	List<MBeneficiarycontact> findByEmailIdOrderByBenContactsIDAsc(String emailId);

	List<MBeneficiarycontact> findByPreferredPhoneNumOrderByBenContactsIDAsc(String phoneNum);

	List<MBeneficiarycontact> findByPreferredSMSPhoneNumOrderByBenContactsIDAsc(String smsPhoneNum);

	
	@Query("select c from MBeneficiarycontact c where c.preferredPhoneNum = :phoneNum ")
	List<MBeneficiarycontact> findByAnyPhoneNum(@Param("phoneNum") String phoneNum);


	@Query("select c from MBeneficiarycontact c where c.preferredPhoneNum = :phoneNum or c.phoneNum1 = :phoneNum "
			+ "or c.phoneNum2 = :phoneNum or c.phoneNum3 = :phoneNum or c.phoneNum4 = :phoneNum "
			+ "or c.phoneNum5 = :phoneNum or c.emergencyContactNum = :phoneNum order by c.benContactsID")
	List<MBeneficiarycontact> findByAllPhoneNum(@Param("phoneNum") String phoneNum);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiarycontact set vanSerialNo = :benContactsID WHERE benContactsID = :benContactsID")
	int updateVanSerialNo(@Param("benContactsID") BigInteger benContactsID);

	@Query("SELECT benContactsID FROM MBeneficiarycontact WHERE vanSerialNo =:vanSerialNo AND vanID =:vanID ")
	BigInteger findIdByVanSerialNoAndVanID(@Param("vanSerialNo") BigInteger vanSerialNo, @Param("vanID") Integer vanID);

	@Query("SELECT a FROM MBeneficiarycontact a WHERE a.vanSerialNo =:vanSerialNo AND a.vanID =:vanID ")
	MBeneficiarycontact getWithVanSerialNoVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);

}
