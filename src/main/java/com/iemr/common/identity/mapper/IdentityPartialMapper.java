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

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import com.iemr.common.identity.domain.identity.MBeneficiarymapping;
import com.iemr.common.identity.dto.BeneficiariesPartialDTO;
import com.iemr.common.identity.dto.BeneficiaryCreateResp;

@Mapper(componentModel = "spring")
public interface IdentityPartialMapper {

	IdentityPartialMapper INSTANCE = Mappers.getMapper(IdentityPartialMapper.class);

	@Mapping(target = "benId", source = "map.MBeneficiaryregidmapping.beneficiaryID")
	@Mapping(target = "benRegId", source = "map.MBeneficiaryregidmapping.benRegId")
	@Mapping(target = "beneficiaryDetailsId", source = "map.MBeneficiarydetail.beneficiaryDetailsId")
	@Mapping(target = "firstName", source = "map.MBeneficiarydetail.firstName")
	@Mapping(target = "lastName", source = "map.MBeneficiarydetail.lastName")
	@Mapping(target = "middleName", source = "map.MBeneficiarydetail.middleName")
	@Mapping(target = "fatherName", source = "map.MBeneficiarydetail.fatherName")
	@Mapping(target = "spouseName", source = "map.MBeneficiarydetail.spouseName")
	@Mapping(target = "beneficiaryAge", expression = "java(MBeneficiarydetail.calculateAge(map.getMBeneficiarydetail().getDob()))")

	BeneficiariesPartialDTO mBeneficiarymappingToBeneficiariesPartialDTO(MBeneficiarymapping map);

	@Mapping(target = "benId", source = "map.MBeneficiaryregidmapping.beneficiaryID")
	@Mapping(target = "benRegId", source = "map.MBeneficiaryregidmapping.benRegId")
	BeneficiaryCreateResp mBeneficiarymappingToBeneficiaryCreateResp(MBeneficiarymapping map);
}
