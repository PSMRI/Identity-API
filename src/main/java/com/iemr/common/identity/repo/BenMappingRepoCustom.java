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

import java.util.List;

import com.iemr.common.identity.domain.identity.MBeneficiarymapping;
import com.iemr.common.identity.domain.identity.VBenAdvanceSearch;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;

public interface BenMappingRepoCustom {

	List<MBeneficiarymapping> dynamicFilterSearch(IdentitySearchDTO searchDTO);

	/**
	 * This method is to search finite beneficiary for MCTS before in order to
	 * generate iemr ID and can be use to finite search generally
	 * 
	 * @param identityDTO
	 * @return mBeneficiarymappingList
	 */
	List<MBeneficiarymapping> finiteSearch(IdentityDTO identityDTO);

	List<VBenAdvanceSearch> dynamicFilterSearchNew(IdentitySearchDTO searchDTO);
}
