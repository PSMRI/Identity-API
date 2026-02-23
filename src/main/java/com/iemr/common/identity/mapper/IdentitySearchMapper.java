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

import com.iemr.common.identity.domain.identity.MBeneficiaryaddress;
import com.iemr.common.identity.domain.identity.MBeneficiarycontact;
import com.iemr.common.identity.domain.identity.MBeneficiarydetail;
import com.iemr.common.identity.dto.IdentitySearchDTO;

@Mapper(componentModel = "spring")
public interface IdentitySearchMapper {

	IdentitySearchMapper INSTANCE = Mappers.getMapper(IdentitySearchMapper.class);

	@Mapping(source = "dto.beneficiaryRegId", target = "beneficiaryRegID")
	@Mapping(source = "dto.fatherName", target = "fatherName")
	@Mapping(source = "dto.firstName", target = "firstName")
	@Mapping(source = "dto.genderId", target = "genderId")
	@Mapping(source = "dto.genderName", target = "gender")
	// @Mapping(source = "dto.ageId", target = "ageId"),
	// @Mapping(source = "dto.age", target = "age"),
	@Mapping(source = "dto.lastName", target = "lastName")
	@Mapping(source = "dto.middleName", target = "middleName")
	@Mapping(source = "dto.spouseName", target = "spouseName")

	MBeneficiarydetail identitySearchDTOMBeneficiarydetail(IdentitySearchDTO dto);

	@Mapping(source = "dto.currentAddress.stateId", target = "currStateId")
	@Mapping(source = "dto.currentAddress.state", target = "currState")
	@Mapping(source = "dto.currentAddress.districtId", target = "currDistrictId")
	@Mapping(source = "dto.currentAddress.district", target = "currDistrict")
	@Mapping(source = "dto.pinCode", target = "currPinCode")
	MBeneficiaryaddress identitySearchDTOToMBeneficiaryaddress(IdentitySearchDTO dto);

	@Mapping(source = "dto.contactNumber", target = "preferredPhoneNum")
	@Mapping(source = "dto.contactNumber", target = "phoneNum1")
	@Mapping(source = "dto.contactNumber", target = "phoneNum2")
	@Mapping(source = "dto.contactNumber", target = "phoneNum3")
	@Mapping(source = "dto.contactNumber", target = "phoneNum4")
	@Mapping(source = "dto.contactNumber", target = "phoneNum5")
	MBeneficiarycontact identitySearchDTOToMBeneficiarycontact(IdentitySearchDTO dto);
}
