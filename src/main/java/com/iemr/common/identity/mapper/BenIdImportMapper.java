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
package com.iemr.common.identity.mapper;

import java.util.ArrayList;
import java.util.List;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import com.iemr.common.identity.domain.MBeneficiaryregidmapping;
import com.iemr.common.identity.dto.BenIdImportDTO;

@Mapper(componentModel = "spring")
public interface BenIdImportMapper {
	BenIdImportMapper INSTANCE = Mappers.getMapper(BenIdImportMapper.class);

	@Mapping(source = "benRegId", target = "benRegId")
	@Mapping(source = "beneficiaryId", target = "beneficiaryID")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	@Mapping(source = "vanID", target = "vanID")
	MBeneficiaryregidmapping benIdImportDTOToMBeneficiaryregidmapping(BenIdImportDTO dto);

	ArrayList<MBeneficiaryregidmapping> benIdImportDTOToMBeneficiaryregidmappings(List<BenIdImportDTO> dto);
}
