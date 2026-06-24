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
package com.iemr.common.identity.repo.familyTag;

import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.data.familyTagging.BenFamilyMapping;


@Repository
public interface FamilyTagRepo extends CrudRepository<BenFamilyMapping,Long> {
	@Transactional
	@Modifying
	@Query("update BenFamilyMapping set deleted=true, "
			+ " modifiedBy=:modifiedBy where benFamilyTagId IN (:benFamilyTagId)")
	public int untagFamily(@Param("benFamilyTagId") List<Long> benFamilyTagId,@Param("modifiedBy") String modifiedBy);
	
	
	@Query("SELECT obj FROM BenFamilyMapping obj WHERE obj.familyName LIKE CONCAT(:familyName, '%') AND (:villageId IS NULL OR obj.villageId =:villageId) AND (obj.deleted IS NULL OR obj.deleted = false)")
	List<BenFamilyMapping> searchFamily(@Param("familyName") String familyName,@Param("villageId") Integer villageId);

	@Query("SELECT obj FROM BenFamilyMapping obj WHERE obj.familyName LIKE CONCAT(:familyName, '%') AND (:villageId IS NULL OR obj.villageId =:villageId) AND obj.familyId =:familyId AND (obj.deleted IS NULL OR obj.deleted = false)")
	List<BenFamilyMapping> searchFamilyWithFamilyId(@Param("familyName") String familyName,@Param("villageId") Integer villageId,@Param("familyId") String familyId);
	
	@Query("SELECT obj FROM BenFamilyMapping obj WHERE obj.familyId =:familyId")
	BenFamilyMapping searchFamilyByFamilyId(@Param("familyId") String familyId);
	
	@Query(value="SELECT userid FROM db_iemr.m_user where username =:username",nativeQuery=true)
	public Integer getUserId(@Param("username") String username);
}
