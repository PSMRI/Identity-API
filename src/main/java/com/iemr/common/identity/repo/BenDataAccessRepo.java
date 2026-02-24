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

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.domain.identity.TBendataaccess;

@Repository
public interface BenDataAccessRepo extends CrudRepository<TBendataaccess, BigInteger>
{
	List<TBendataaccess> findByAccessIdOrderByAccessIdAsc(BigInteger accessId);

	List<TBendataaccess> findByUserAgentIdOrderByAccessIdAsc(Integer userAgentId);

	List<TBendataaccess> findByUserAgentIdAndAccessedOnBetweenOrderByAccessIdAsc(Integer userAgentId,
			Timestamp fromDate, Timestamp toDate);


	List<TBendataaccess> findByUserAgentPSMapIdOrderByAccessIdAsc(Integer userAgentPSMapId);

	List<TBendataaccess> findByUserAgentRoleIdOrderByAccessIdAsc(Integer userAgentRoleId);

	List<TBendataaccess> findByUserAgentServiceIdOrderByAccessIdAsc(Integer userAgentServiceId);

	List<TBendataaccess> findByUserAgentPSMapIdAndAccessedOnBetweenOrderByAccessIdAsc(Integer userAgentPSMapId,
			Timestamp fromDate, Timestamp toDate);

	List<TBendataaccess> findByUserAgentRoleIdAndAccessedOnBetweenOrderByAccessIdAsc(Integer userAgentRoleId,
			Timestamp fromDate, Timestamp toDate);

	List<TBendataaccess> findByUserAgentServiceIdAndAccessedOnBetweenOrderByAccessIdAsc(Integer userAgentServiceId,
			Timestamp fromDate, Timestamp toDate);

	Long countByUserAgentIdAndAccessedOnBetweenOrderByAccessIdAsc(Integer userAgentId, Timestamp fromDate, Timestamp toDate);

	Long countByUserAgentPSMapId(Integer userAgentPSMapId);

	Long countByUserAgentRoleId(Integer userAgentRoleId);

	Long countByUserAgentServiceId(Integer userAgentServiceId);

	Long countByUserAgentPSMapIdAndAccessedOnBetween(Integer userAgentPSMapId, Timestamp fromDate, Timestamp toDate);

	Long countByUserAgentRoleIdAndAccessedOnBetween(Integer userAgentRoleId, Timestamp fromDate, Timestamp toDate);

	Long countByUserAgentServiceIdAndAccessedOnBetween(Integer userAgentServiceId, Timestamp fromDate,
			Timestamp toDate);

	List<TBendataaccess> findByaccessedOnBetweenOrderByAccessIdAsc(Timestamp fromDate, Timestamp toDate);
}
