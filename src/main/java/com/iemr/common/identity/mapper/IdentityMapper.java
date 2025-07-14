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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.mapstruct.InheritInverseConfiguration;
import org.mapstruct.IterableMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.Named;
import org.mapstruct.factory.Mappers;

import com.iemr.common.identity.domain.Identity;
import com.iemr.common.identity.domain.MBeneficiaryAccount;
import com.iemr.common.identity.domain.MBeneficiaryImage;
import com.iemr.common.identity.domain.MBeneficiaryaddress;
import com.iemr.common.identity.domain.MBeneficiaryconsent;
import com.iemr.common.identity.domain.MBeneficiarycontact;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiaryfamilymapping;
import com.iemr.common.identity.domain.MBeneficiaryidentity;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.domain.MBeneficiaryservicemapping;
import com.iemr.common.identity.domain.Phone;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.BenIdentityDTO;
import com.iemr.common.identity.dto.BenServiceDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.IdentityDTO;

@Mapper(componentModel = "spring", imports = { Phone.class })
public interface IdentityMapper {
	IdentityMapper INSTANCE = Mappers.getMapper(IdentityMapper.class);

	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.createdDate", target = "createdDate")

	@Mapping(source = "dto.vanID", target = "vanID")
	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")

	MBeneficiarymapping identityDTOToMBeneficiarymapping(IdentityDTO dto);

	

	@Mapping(source = "defaultNo", target = "shareAnonymousWithGovt")
	@Mapping(source = "defaultNo", target = "shareAnonymousWithMedicalCommunity")
	@Mapping(source = "defaultNo", target = "shareAnonymousWithNGO")
	@Mapping(source = "defaultNo", target = "shareMedicalDetailsForMedicalStudy")
	@Mapping(source = "defaultYes", target = "shareMedicalDetailsWithDoctor")
	@Mapping(source = "defaultYes", target = "shareMedicalDetailsWithFamily")
	@Mapping(source = "defaultNo", target = "shareMedicalDetailsWithFriends")
	@Mapping(source = "defaultNo", target = "shareMedicalDetailsWithGovt")
	@Mapping(source = "defaultNo", target = "shareMedicalDetailsWithNGO")
	@Mapping(source = "defaultYes", target = "shareMedicalDetailsWithSpouse")
	@Mapping(source = "defaultNo", target = "sharePersonalDetailsForMedicalStudy")
	@Mapping(source = "defaultYes", target = "sharePersonalDetailsWithDoctor")
	@Mapping(source = "defaultNo", target = "sharePersonalDetailsWithFamily")
	@Mapping(source = "defaultNo", target = "sharePersonalDetailsWithFriends")
	@Mapping(source = "defaultNo", target = "sharePersonalDetailsWithGovt")
	@Mapping(source = "defaultNo", target = "sharePersonalDetailsWithNGO")
	@Mapping(source = "defaultYes", target = "sharePersonalDetailsWithSpouse")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.createdDate", target = "createdDate")

	@Mapping(source = "dto.vanID", target = "vanID")
	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")

	MBeneficiaryconsent identityDTOToDefaultMBeneficiaryconsent(IdentityDTO dto, Boolean defaultYes, Boolean defaultNo);

	

//	@Mapping(source = "dto.areaId", target = "areaId")
//	@Mapping(source = "dto.beneficiaryRegId", target = "beneficiaryRegID")
//	@Mapping(source = "dto.community", target = "community")
//	@Mapping(source = "dto.communityId", target = "communityId")
//	@Mapping(source = "dto.dob", target = "dob")
//	@Mapping(source = "dto.education", target = "education")
//	@Mapping(source = "dto.educationId", target = "educationId")
//	@Mapping(source = "dto.emergencyRegistration", target = "emergencyRegistration")
//	@Mapping(source = "dto.healthCareWorkerId", target = "healthCareWorkerId")
//	@Mapping(source = "dto.healthCareWorker", target = "healthCareWorker")
//	@Mapping(source = "dto.fatherName", target = "fatherName")
//	@Mapping(source = "dto.motherName", target = "motherName")
//	@Mapping(source = "dto.firstName", target = "firstName")
//	@Mapping(source = "dto.gender", target = "gender")
//	@Mapping(source = "dto.genderId", target = "genderId")
//	@Mapping(source = "dto.incomeStatus", target = "incomeStatus")
//	@Mapping(source = "dto.monthlyFamilyIncome", target = "monthlyFamilyIncome")
//	@Mapping(source = "dto.incomeStatusId", target = "incomeStatusId")
//	@Mapping(source = "dto.lastName", target = "lastName")
//	@Mapping(source = "dto.maritalStatusId", target = "maritalStatusId")
//	@Mapping(source = "dto.maritalStatus", target = "maritalStatus")
//	@Mapping(source = "dto.middleName", target = "middleName")
//	@Mapping(source = "dto.occupation", target = "occupation")
//	@Mapping(source = "dto.occupationId", target = "occupationId")
//	@Mapping(source = "dto.phcId", target = "phcId")
//	@Mapping(source = "dto.placeOfWork", target = "placeOfWork")
//	@Mapping(source = "dto.preferredLanguage", target = "preferredLanguage")
//	@Mapping(source = "dto.religion", target = "religion")
//	@Mapping(source = "dto.religionId", target = "religionId")
//	@Mapping(source = "dto.remarks", target = "remarks")
//	@Mapping(source = "dto.servicePointId", target = "servicePointId")
//	@Mapping(source = "dto.sourceOfInfo", target = "sourceOfInfo")
//	@Mapping(source = "dto.spouseName", target = "spouseName")
//	@Mapping(source = "dto.status", target = "status")
//	@Mapping(source = "dto.title", target = "title")
//	@Mapping(source = "dto.titleId", target = "titleId")
//	@Mapping(source = "dto.zoneId", target = "zoneId")
//	@Mapping(source = "dto.agentName", target = "createdBy")
//	@Mapping(source = "dto.createdDate", target = "createdDate")
//	@Mapping(target = "isHIVPositive", expression = "java(MBeneficiarydetail.setIsHIVPositive(dto.getIsHIVPositive()))")
//
//	@Mapping(target = "ageAtMarriage", expression = "java(MBeneficiarydetail.getAgeAtMarriageCalc(dto.getDob(), dto.getMarriageDate(), "
//			+ "dto.getAgeAtMarriage()))")
//	@Mapping(target = "marriageDate", expression = "java(MBeneficiarydetail.getMarriageDateCalc(dto.getDob(), dto.getMarriageDate(), "
//			+ "dto.getAgeAtMarriage()))")
//
//	@Mapping(source = "dto.vanID", target = "vanID")
//	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")
//	MBeneficiarydetail identityDTOToMBeneficiarydetail(IdentityDTO dto);
	

	@Mapping(source = "benFamilyDTO.isEmergencyContact", target = "isEmergencyContact")
	@Mapping(source = "benFamilyDTO.relationshipToSelf", target = "relationshipToSelf")
	@Mapping(source = "benFamilyDTO.associatedBenRegId", target = "associatedBenRegId")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	MBeneficiaryfamilymapping identityDTOToMBeneficiaryfamilymapping(BenFamilyDTO benFamilyDTO, String createdBy,
			Timestamp createdDate);

	List<MBeneficiaryfamilymapping> identityDTOListToMBeneficiaryfamilymappingList(List<BenFamilyDTO> list);

	@Mapping(source = "identity.identityNo", target = "identityNo")
	@Mapping(source = "identity.identityNameId", target = "identityNameId")
	@Mapping(source = "identity.identityName", target = "identityName")
	@Mapping(source = "identity.identityTypeId", target = "identityTypeId")
	@Mapping(source = "identity.identityType", target = "identityType")
	@Mapping(source = "identity.issueDate", target = "issueDate")
	@Mapping(source = "identity.expiryDate", target = "expiryDate")
	@Mapping(source = "identity.isVerified", target = "isVerified")
	@Mapping(source = "identity.identityFilePath", target = "identityFilePath")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	MBeneficiaryidentity identityToMBeneficiaryidentity(Identity identity, String createdBy, Timestamp createdDate);

	List<MBeneficiaryidentity> identityDTOListToMBeneficiaryidentityList(List<Identity> list);

	@Mapping(source = "dto.serviceId", target = "serviceId")
	@Mapping(source = "dto.serviceName", target = "serviceName")
	@Mapping(source = "dto.stateId", target = "stateId")
	@Mapping(source = "dto.stateName", target = "stateName")
	@Mapping(source = "dto.serviceProviderId", target = "serviceProviderId")
	@Mapping(source = "dto.serviceProviderName", target = "serviceProviderName")
	@Mapping(source = "dto.providerServiceMapId", target = "providerServiceMapId")
	@Mapping(source = "dto.agentId", target = "registeredById")
	@Mapping(source = "dto.agentName", target = "registeredByName")
	@Mapping(source = "dto.eventTypeDate", target = "registeredDate")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.createdDate", target = "createdDate")
	@Mapping(source = "dto.vanID", target = "vanID")
	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")
	MBeneficiaryservicemapping identityDTOToMBeneficiaryservicemapping(IdentityDTO dto);

	@Mapping(target = "benMapId", source = "benMapId")
	@Mapping(target = "benId", source = "map.MBeneficiaryregidmapping.beneficiaryID")
	@Mapping(target = "benRegId", source = "map.MBeneficiaryregidmapping.benRegId")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	@Mapping(source = "lastModDate", target = "lastModDate")
	@Mapping(source = "modifiedBy", target = "modifiedBy")
	@Mapping(source = "deleted", target = "deleted")
	@Mapping(target = "currentAddress.addrLine1", source = "map.MBeneficiaryaddress.currAddrLine1")
	@Mapping(target = "currentAddress.addrLine2", source = "map.MBeneficiaryaddress.currAddrLine2")
	@Mapping(target = "currentAddress.addrLine3", source = "map.MBeneficiaryaddress.currAddrLine3")
	@Mapping(target = "currentAddress.countryId", source = "map.MBeneficiaryaddress.currCountryId")
	@Mapping(target = "currentAddress.country", source = "map.MBeneficiaryaddress.currCountry")
	@Mapping(target = "currentAddress.stateId", source = "map.MBeneficiaryaddress.currStateId")
	@Mapping(target = "currentAddress.state", source = "map.MBeneficiaryaddress.currState")
	@Mapping(target = "currentAddress.districtId", source = "map.MBeneficiaryaddress.currDistrictId")
	@Mapping(target = "currentAddress.district", source = "map.MBeneficiaryaddress.currDistrict")
	@Mapping(target = "currentAddress.subDistrictId", source = "map.MBeneficiaryaddress.currSubDistrictId")
	@Mapping(target = "currentAddress.subDistrict", source = "map.MBeneficiaryaddress.currSubDistrict")
	@Mapping(target = "currentAddress.villageId", source = "map.MBeneficiaryaddress.currVillageId")
	@Mapping(target = "currentAddress.village", source = "map.MBeneficiaryaddress.currVillage")
	@Mapping(target = "currentAddress.addressValue", source = "map.MBeneficiaryaddress.currAddressValue")
	@Mapping(target = "currentAddress.pinCode", source = "map.MBeneficiaryaddress.currPinCode")
	@Mapping(target = "emergencyAddress.addrLine1", source = "map.MBeneficiaryaddress.emerAddrLine1")
	@Mapping(target = "emergencyAddress.addrLine2", source = "map.MBeneficiaryaddress.emerAddrLine2")
	@Mapping(target = "emergencyAddress.addrLine3", source = "map.MBeneficiaryaddress.emerAddrLine3")
	@Mapping(target = "emergencyAddress.countryId", source = "map.MBeneficiaryaddress.emerCountryId")
	@Mapping(target = "emergencyAddress.country", source = "map.MBeneficiaryaddress.emerCountry")
	@Mapping(target = "emergencyAddress.stateId", source = "map.MBeneficiaryaddress.emerStateId")
	@Mapping(target = "emergencyAddress.state", source = "map.MBeneficiaryaddress.emerState")
	@Mapping(target = "emergencyAddress.districtId", source = "map.MBeneficiaryaddress.emerDistrictId")
	@Mapping(target = "emergencyAddress.district", source = "map.MBeneficiaryaddress.emerDistrict")
	@Mapping(target = "emergencyAddress.subDistrictId", source = "map.MBeneficiaryaddress.emerSubDistrictId")
	@Mapping(target = "emergencyAddress.subDistrict", source = "map.MBeneficiaryaddress.emerSubDistrict")
	@Mapping(target = "emergencyAddress.villageId", source = "map.MBeneficiaryaddress.emerVillageId")
	@Mapping(target = "emergencyAddress.village", source = "map.MBeneficiaryaddress.emerVillage")
	@Mapping(target = "emergencyAddress.addressValue", source = "map.MBeneficiaryaddress.emerAddressValue")
	@Mapping(target = "emergencyAddress.pinCode", source = "map.MBeneficiaryaddress.emerPinCode")
	@Mapping(target = "permanentAddress.addrLine1", source = "map.MBeneficiaryaddress.permAddrLine1")
	@Mapping(target = "permanentAddress.addrLine2", source = "map.MBeneficiaryaddress.permAddrLine2")
	@Mapping(target = "permanentAddress.addrLine3", source = "map.MBeneficiaryaddress.permAddrLine3")
	@Mapping(target = "permanentAddress.countryId", source = "map.MBeneficiaryaddress.permCountryId")
	@Mapping(target = "permanentAddress.country", source = "map.MBeneficiaryaddress.permCountry")
	@Mapping(target = "permanentAddress.stateId", source = "map.MBeneficiaryaddress.permStateId")
	@Mapping(target = "permanentAddress.state", source = "map.MBeneficiaryaddress.permState")
	@Mapping(target = "permanentAddress.districtId", source = "map.MBeneficiaryaddress.permDistrictId")
	@Mapping(target = "permanentAddress.district", source = "map.MBeneficiaryaddress.permDistrict")
	@Mapping(target = "permanentAddress.subDistrictId", source = "map.MBeneficiaryaddress.permSubDistrictId")
	@Mapping(target = "permanentAddress.subDistrict", source = "map.MBeneficiaryaddress.permSubDistrict")
	@Mapping(target = "permanentAddress.villageId", source = "map.MBeneficiaryaddress.permVillageId")
	@Mapping(target = "permanentAddress.village", source = "map.MBeneficiaryaddress.permVillage")
	@Mapping(target = "permanentAddress.addressValue", source = "map.MBeneficiaryaddress.permAddressValue")
	@Mapping(target = "permanentAddress.pinCode", source = "map.MBeneficiaryaddress.permPinCode")
	@Mapping(target = "preferredPhoneNum", source = "map.MBeneficiarycontact.preferredPhoneNum")
	@Mapping(target = "preferredPhoneTyp", source = "map.MBeneficiarycontact.preferredPhoneTyp")
	@Mapping(target = "preferredSMSPhoneNum", source = "map.MBeneficiarycontact.preferredSMSPhoneNum")
	@Mapping(target = "preferredSMSPhoneTyp", source = "map.MBeneficiarycontact.preferredSMSPhoneTyp")
	@Mapping(target = "emergencyContactNum", source = "map.MBeneficiarycontact.emergencyContactNum")
	@Mapping(target = "emergencyContactTyp", source = "map.MBeneficiarycontact.emergencyContactTyp")
	@Mapping(target = "preferredEmailId", source = "map.MBeneficiarycontact.emailId") // beneficiaryDetails.firstName
																						// map.MBeneficiarydetail.firstName
	@Mapping(target = "beneficiaryDetails.beneficiaryDetailsId", source = "map.MBeneficiarydetail.beneficiaryDetailsId")
	@Mapping(target = "beneficiaryDetails.areaId", source = "map.MBeneficiarydetail.areaId")
	@Mapping(target = "beneficiaryDetails.beneficiaryRegID", source = "map.MBeneficiarydetail.beneficiaryRegID")
	@Mapping(target = "beneficiaryDetails.community", source = "map.MBeneficiarydetail.community")
	@Mapping(target = "beneficiaryDetails.createdBy", source = "map.MBeneficiarydetail.createdBy")
	@Mapping(target = "beneficiaryDetails.createdDate", source = "map.MBeneficiarydetail.createdDate")
	@Mapping(target = "beneficiaryDetails.deleted", source = "map.MBeneficiarydetail.deleted")
	@Mapping(target = "beneficiaryDetails.dob", source = "map.MBeneficiarydetail.dob")
	@Mapping(target = "beneficiaryDetails.education", source = "map.MBeneficiarydetail.education")
	@Mapping(target = "beneficiaryDetails.emergencyRegistration", source = "map.MBeneficiarydetail.emergencyRegistration")
	@Mapping(target = "beneficiaryDetails.fatherName", source = "map.MBeneficiarydetail.fatherName")
	@Mapping(target = "beneficiaryDetails.motherName", source = "map.MBeneficiarydetail.motherName")
	@Mapping(target = "beneficiaryDetails.firstName", source = "map.MBeneficiarydetail.firstName")
	@Mapping(target = "beneficiaryDetails.gender", source = "map.MBeneficiarydetail.gender")
	@Mapping(target = "beneficiaryDetails.incomeStatus", source = "map.MBeneficiarydetail.incomeStatus")
	@Mapping(target = "beneficiaryDetails.monthlyFamilyIncome", source = "map.MBeneficiarydetail.monthlyFamilyIncome")
	@Mapping(target = "beneficiaryDetails.lastModDate", source = "map.MBeneficiarydetail.lastModDate")
	@Mapping(target = "beneficiaryDetails.lastName", source = "map.MBeneficiarydetail.lastName")
	@Mapping(target = "beneficiaryDetails.maritalStatus", source = "map.MBeneficiarydetail.maritalStatus")
	@Mapping(target = "beneficiaryDetails.middleName", source = "map.MBeneficiarydetail.middleName")
	@Mapping(target = "beneficiaryDetails.modifiedBy", source = "map.MBeneficiarydetail.modifiedBy")
	@Mapping(target = "beneficiaryDetails.occupation", source = "map.MBeneficiarydetail.occupation")
	@Mapping(target = "beneficiaryDetails.phcId", source = "map.MBeneficiarydetail.phcId")
	@Mapping(target = "beneficiaryDetails.occupationId", source = "map.MBeneficiarydetail.occupationId")
	@Mapping(target = "beneficiaryDetails.religionId", source = "map.MBeneficiarydetail.religionId")
	@Mapping(target = "beneficiaryDetails.genderId", source = "map.MBeneficiarydetail.genderId")
	@Mapping(target = "beneficiaryDetails.maritalStatusId", source = "map.MBeneficiarydetail.maritalStatusId")
	@Mapping(target = "beneficiaryDetails.titleId", source = "map.MBeneficiarydetail.titleId")
	@Mapping(target = "beneficiaryDetails.communityId", source = "map.MBeneficiarydetail.communityId")
	@Mapping(target = "beneficiaryDetails.educationId", source = "map.MBeneficiarydetail.educationId")
	@Mapping(target = "beneficiaryDetails.sexualOrientationID", source = "map.MBeneficiarydetail.sexualOrientationID")
	@Mapping(target = "beneficiaryDetails.sexualOrientationType", source = "map.MBeneficiarydetail.sexualOrientationType")
	@Mapping(target = "beneficiaryDetails.isHIVPositive", expression = "java(MBeneficiarydetail.getIsHIVPositive(mBeneficiarydetail.getIsHIVPositive()))")
	@Mapping(target = "beneficiaryDetails.placeOfWork", source = "map.MBeneficiarydetail.placeOfWork")
	@Mapping(target = "beneficiaryDetails.healthCareWorkerId", source = "map.MBeneficiarydetail.healthCareWorkerId")
	@Mapping(target = "beneficiaryDetails.healthCareWorker", source = "map.MBeneficiarydetail.healthCareWorker")
	@Mapping(target = "beneficiaryDetails.preferredLanguageId", source = "map.MBeneficiarydetail.preferredLanguageId")
	@Mapping(target = "beneficiaryDetails.preferredLanguage", source = "map.MBeneficiarydetail.preferredLanguage")

	@Mapping(target = "beneficiaryDetails.religion", source = "map.MBeneficiarydetail.religion")
	@Mapping(target = "beneficiaryDetails.remarks", source = "map.MBeneficiarydetail.remarks")

	@Mapping(target = "beneficiaryDetails.servicePointId", source = "map.MBeneficiarydetail.servicePointId")
	@Mapping(target = "beneficiaryDetails.sourceOfInfo", source = "map.MBeneficiarydetail.sourceOfInfo")
	@Mapping(target = "beneficiaryDetails.spouseName", source = "map.MBeneficiarydetail.spouseName")
	@Mapping(target = "beneficiaryDetails.status", source = "map.MBeneficiarydetail.status")
	@Mapping(target = "beneficiaryDetails.title", source = "map.MBeneficiarydetail.title")
	@Mapping(target = "beneficiaryDetails.zoneId", source = "map.MBeneficiarydetail.zoneId")
	@Mapping(target = "contacts", expression = "java( map != null && map.getMBeneficiarycontact() != null && "
            + "map.getMBeneficiarydetail() != null ? "
            + "Phone.createContactList(map.getMBeneficiarycontact(), "
            + "(benRegId != null ? benRegId.toString() : null), "
            + "(map.getMBeneficiarydetail().getFirstName() != null ? map.getMBeneficiarydetail().getFirstName() : \"\") + \" \" + "
            + "(map.getMBeneficiarydetail().getMiddleName() != null ? map.getMBeneficiarydetail().getMiddleName() : \"\") + \" \" + "
            + "(map.getMBeneficiarydetail().getLastName() != null ? map.getMBeneficiarydetail().getLastName() : \"\") "
            + ") : null)")

	@Mapping(target = "permanentAddress.zoneID", source = "map.MBeneficiaryaddress.permZoneID")
	@Mapping(target = "permanentAddress.zoneName", source = "map.MBeneficiaryaddress.permZone")
	@Mapping(target = "permanentAddress.parkingPlaceID", source = "map.MBeneficiaryaddress.permAreaId")
	@Mapping(target = "permanentAddress.parkingPlaceName", source = "map.MBeneficiaryaddress.permArea")
	@Mapping(target = "permanentAddress.servicePointID", source = "map.MBeneficiaryaddress.permServicePointId")
	@Mapping(target = "permanentAddress.servicePointName", source = "map.MBeneficiaryaddress.permServicePoint")
	@Mapping(target = "permanentAddress.habitation", source = "map.MBeneficiaryaddress.permHabitation")

	@Mapping(target = "currentAddress.zoneID", source = "map.MBeneficiaryaddress.currZoneID")
	@Mapping(target = "currentAddress.zoneName", source = "map.MBeneficiaryaddress.currZone")
	@Mapping(target = "currentAddress.parkingPlaceID", source = "map.MBeneficiaryaddress.currAreaId")
	@Mapping(target = "currentAddress.parkingPlaceName", source = "map.MBeneficiaryaddress.currArea")
	@Mapping(target = "currentAddress.servicePointID", source = "map.MBeneficiaryaddress.currServicePointId")
	@Mapping(target = "currentAddress.servicePointName", source = "map.MBeneficiaryaddress.currServicePoint")
	@Mapping(target = "currentAddress.habitation", source = "map.MBeneficiaryaddress.currHabitation")

	@Mapping(target = "emergencyAddress.zoneID", source = "map.MBeneficiaryaddress.emerZoneID")
	@Mapping(target = "emergencyAddress.zoneName", source = "map.MBeneficiaryaddress.emerZone")
	@Mapping(target = "emergencyAddress.parkingPlaceID", source = "map.MBeneficiaryaddress.emerAreaId")
	@Mapping(target = "emergencyAddress.parkingPlaceName", source = "map.MBeneficiaryaddress.emerArea")
	@Mapping(target = "emergencyAddress.servicePointID", source = "map.MBeneficiaryaddress.emerServicePointId")
	@Mapping(target = "emergencyAddress.servicePointName", source = "map.MBeneficiaryaddress.emerServicePoint")
	@Mapping(target = "emergencyAddress.habitation", source = "map.MBeneficiaryaddress.emerHabitation")

	@Mapping(target = "bankName", source = "map.MBeneficiaryAccount.bankName")
	@Mapping(target = "branchName", source = "map.MBeneficiaryAccount.branchName")
	@Mapping(target = "ifscCode", source = "map.MBeneficiaryAccount.ifscCode")
	@Mapping(target = "accountNo", source = "map.MBeneficiaryAccount.accountNo")
	@Mapping(target = "benAccountID", source = "map.benAccountID")
	@Mapping(target = "ageAtMarriage", expression = "java(map != null && map.getMBeneficiarydetail() != null ? "
            + "MBeneficiarydetail.getAgeAtMarriageCalc(map.getMBeneficiarydetail().getDob(), "
            + "map.getMBeneficiarydetail().getMarriageDate(), "
            + "map.getMBeneficiarydetail().getAgeAtMarriage()) : null)")
	@Mapping(target = "marriageDate", expression = "java(map != null && map.getMBeneficiarydetail() != null ? "
            + "MBeneficiarydetail.getMarriageDateCalc(map.getMBeneficiarydetail().getDob(), "
            + "map.getMBeneficiarydetail().getMarriageDate(), "
            + "map.getMBeneficiarydetail().getAgeAtMarriage()) : null)")

	@Mapping(target = "literacyStatus", source = "map.MBeneficiarydetail.literacyStatus")
	@Mapping(target = "motherName", source = "map.MBeneficiarydetail.motherName")
	@Mapping(target = "email", source = "map.MBeneficiarycontact.emailId")

	@Mapping(source = "map.MBeneficiarydetail.occupationId", target = "occupationId")
	@Mapping(source = "map.MBeneficiarydetail.occupation", target = "occupationName")
	@Mapping(source = "map.MBeneficiarydetail.incomeStatus", target = "incomeStatus")
	@Mapping(source = "map.MBeneficiarydetail.monthlyFamilyIncome", target = "monthlyFamilyIncome")
	@Mapping(source = "map.MBeneficiarydetail.religionId", target = "religionId")
	@Mapping(source = "map.MBeneficiarydetail.religion", target = "religion")
	// End

	// D2D, 20-01-2021
	@Mapping(target = "beneficiaryDetails.houseHoldID", source = "map.houseHoldID")
	@Mapping(target = "beneficiaryDetails.guideLineID", source = "map.guideLineID")
	@Mapping(target = "beneficiaryDetails.rchID", source = "map.rchID")

	// family id in response
	@Mapping(target = "beneficiaryDetails.headOfFamily_RelationID", source = "map.MBeneficiarydetail.headOfFamily_RelationID")
	@Mapping(target = "beneficiaryDetails.familyId", source = "map.MBeneficiarydetail.familyId")
	@Mapping(target = "beneficiaryDetails.other", source = "map.MBeneficiarydetail.other")
	@Mapping(target = "beneficiaryDetails.headOfFamily_Relation", source = "map.MBeneficiarydetail.headOfFamily_Relation")

	// Start 1097
	@Mapping(expression = "java(map != null && map.getMBeneficiarydetail() != null ? MBeneficiarydetail.calculateAge(map.getMBeneficiarydetail().getDob()) : null)", target = "beneficiaryAge")// End 1097

	BeneficiariesDTO mBeneficiarymappingToBeneficiariesDTO(MBeneficiarymapping map);

	@IterableMapping(elementTargetType = BeneficiariesDTO.class)
	List<BeneficiariesDTO> mbeneficiarymappingToBeneficiariesDTO(List<MBeneficiarymapping> map);

	@Mapping(source = "beneficiaryDetailsId", target = "beneficiaryDetailsId")
	@Mapping(source = "areaId", target = "areaId")
	@Mapping(source = "beneficiaryRegID", target = "beneficiaryRegID")
	@Mapping(source = "community", target = "community")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	@Mapping(source = "deleted", target = "deleted")
	@Mapping(source = "dob", target = "dob")
	@Mapping(source = "education", target = "education")
	@Mapping(source = "emergencyRegistration", target = "emergencyRegistration")
	@Mapping(source = "fatherName", target = "fatherName")
	@Mapping(source = "motherName", target = "motherName")
	@Mapping(source = "firstName", target = "firstName")
	@Mapping(source = "gender", target = "gender")
	@Mapping(source = "incomeStatus", target = "incomeStatus")
	@Mapping(source = "lastModDate", target = "lastModDate")
	@Mapping(source = "lastName", target = "lastName")
	@Mapping(source = "maritalStatus", target = "maritalStatus")
	@Mapping(source = "middleName", target = "middleName")
	@Mapping(source = "modifiedBy", target = "modifiedBy")
	@Mapping(source = "occupation", target = "occupation")
	@Mapping(source = "phcId", target = "phcId")
	@Mapping(source = "placeOfWork", target = "placeOfWork")
	@Mapping(source = "preferredLanguageId", target = "preferredLanguageId")
	@Mapping(source = "preferredLanguage", target = "preferredLanguage")
	// @Mapping(source = "religionId", target = "religionId"),
	@Mapping(source = "religion", target = "religion")
	@Mapping(source = "remarks", target = "remarks")
	@Mapping(source = "servicePointId", target = "servicePointId")
	@Mapping(source = "sourceOfInfo", target = "sourceOfInfo")
	@Mapping(source = "spouseName", target = "spouseName")
	@Mapping(source = "status", target = "status")
	@Mapping(source = "title", target = "title")
	@Mapping(source = "zoneId", target = "zoneId")
	@Mapping(expression = "java(MBeneficiarydetail.getIsHIVPositive(detail.getIsHIVPositive()))", target = "isHIVPositive")
	BenDetailDTO mBeneficiarydetailToBenDetailDTO(MBeneficiarydetail detail);

	@Mapping(source = "family.benFamilyMapId", target = "benFamilyMapId")
	@Mapping(source = "family.associatedBenRegId", target = "associatedBenRegId")
	@Mapping(source = "family.createdBy", target = "createdBy")
	@Mapping(source = "family.createdDate", target = "createdDate")
	@Mapping(source = "family.deleted", target = "deleted")
	@Mapping(source = "family.isEmergencyContact", target = "isEmergencyContact")
	@Mapping(source = "family.lastModDate", target = "lastModDate")
	@Mapping(source = "family.modifiedBy", target = "modifiedBy")
	@Mapping(source = "family.relationshipToSelf", target = "relationshipToSelf")
	BenFamilyDTO mBeneficiaryfamilymappingToBenFamilyDTO(MBeneficiaryfamilymapping family);

	List<BenFamilyDTO> mBeneficiaryfamilymappingListToBenFamilyDTOList(List<MBeneficiaryfamilymapping> families);

	BenFamilyDTO mapToBenFamilyDTO(BenFamilyDTO benFamilyDTO);

	MBeneficiaryfamilymapping mapToMBeneficiaryfamilymapping(MBeneficiaryfamilymapping beneficiaryfamilymapping);

	@IterableMapping(elementTargetType = MBeneficiaryfamilymapping.class)
	List<MBeneficiaryfamilymapping> mapToMBeneficiaryfamilymappingList(
			Collection<MBeneficiaryfamilymapping> beneficiaryfamilymappings);

	@IterableMapping(elementTargetType = BenFamilyDTO.class)
	List<BenFamilyDTO> mapToMBeneficiaryfamilymappingWithBenFamilyDTOList(
			List<MBeneficiaryfamilymapping> beneficiaryfamilymappings);

	@Mapping(source = "benIdentityId", target = "benIdentityId")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	@Mapping(source = "deleted", target = "deleted")
	@Mapping(source = "expiryDate", target = "expiryDate")
	@Mapping(source = "identityFilePath", target = "identityFilePath")
	@Mapping(source = "identityName", target = "identityName")
	@Mapping(source = "identityNameId", target = "identityNameId")
	@Mapping(source = "identityNo", target = "identityNo")
	@Mapping(source = "identityTypeId", target = "identityTypeId")
	@Mapping(source = "identityType", target = "identityType")
	@Mapping(source = "issueDate", target = "issueDate")
	@Mapping(source = "isVerified", target = "isVerified")
	@Mapping(source = "lastModDate", target = "lastModDate")
	@Mapping(source = "modifiedBy", target = "modifiedBy")
	BenIdentityDTO mBeneficiaryidentityToBenIdentityDTO(MBeneficiaryidentity identity);

	List<BenIdentityDTO> mBeneficiaryidentityListToBenIdentityDTOList(List<MBeneficiaryidentity> identity);

	@Mapping(source = "benServiceMapID", target = "benServiceMapID")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	@Mapping(source = "firstAvailedOn", target = "firstAvailedOn")
	@Mapping(source = "lastModDate", target = "lastModDate")
	@Mapping(source = "modifiedBy", target = "modifiedBy")
	@Mapping(source = "registeredByName", target = "registeredByName")
	@Mapping(source = "registeredById", target = "registeredById")
	@Mapping(source = "registeredDate", target = "registeredDate")
	@Mapping(source = "providerServiceMapId", target = "providerServiceMapId")
	@Mapping(source = "serviceId", target = "serviceId")
	@Mapping(source = "serviceName", target = "serviceName")
	@Mapping(source = "serviceProviderId", target = "serviceProviderId")
	@Mapping(source = "serviceProviderName", target = "serviceProviderName")
	@Mapping(source = "stateId", target = "stateId")
	@Mapping(source = "stateName", target = "stateName")
	BenServiceDTO mBeneficiaryservicemappingToBenServiceDTO(MBeneficiaryservicemapping service);

	List<BenServiceDTO> mBeneficiaryservicemappingListToBenServiceDTOList(List<MBeneficiaryservicemapping> services);

	@Mapping(source = "dto.bankName", target = "bankName")
	@Mapping(source = "dto.branchName", target = "branchName")
	@Mapping(source = "dto.ifscCode", target = "ifscCode")
	@Mapping(source = "dto.accountNo", target = "accountNo")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.createdDate", target = "createdDate")
	@Mapping(source = "dto.vanID", target = "vanID")
	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")
	// End
	MBeneficiaryAccount identityDTOToMBeneficiaryAccount(IdentityDTO dto);

	@InheritInverseConfiguration
	IdentityDTO mBeneficiaryAccountToIdentityDTO(MBeneficiaryAccount dto);

	@Mapping(source = "dto.benImage", target = "benImage")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.createdDate", target = "createdDate")
	
	@Mapping(source = "dto.vanID", target = "vanID")
	@Mapping(source = "dto.parkingPlaceId", target = "parkingPlaceID")

	MBeneficiaryImage identityDTOToMBeneficiaryImage(IdentityDTO dto);

	@InheritInverseConfiguration
	IdentityDTO mBeneficiaryImageToIdentityDTO(MBeneficiaryImage dto);

	// End outreach
}
