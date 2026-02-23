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
import java.util.Collection;
import java.util.List;

import org.mapstruct.IterableMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import com.iemr.common.identity.domain.identity.Identity;
import com.iemr.common.identity.domain.identity.MBeneficiaryAccount;
import com.iemr.common.identity.domain.identity.MBeneficiaryImage;
import com.iemr.common.identity.domain.identity.MBeneficiaryaddress;
import com.iemr.common.identity.domain.identity.MBeneficiaryconsent;
import com.iemr.common.identity.domain.identity.MBeneficiarycontact;
import com.iemr.common.identity.domain.identity.MBeneficiarydetail;
import com.iemr.common.identity.domain.identity.MBeneficiaryfamilymapping;
import com.iemr.common.identity.domain.identity.MBeneficiaryidentity;
import com.iemr.common.identity.domain.identity.MBeneficiarymapping;
import com.iemr.common.identity.domain.identity.MBeneficiaryservicemapping;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.BenIdentityDTO;
import com.iemr.common.identity.dto.BenServiceDTO;
import com.iemr.common.identity.dto.IdentityEditDTO;

@Mapper(componentModel = "spring")

public interface IdentityEditMapper {

	IdentityEditMapper INSTANCE = Mappers.getMapper(IdentityEditMapper.class);

	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.eventTypeDate", target = "lastModDate")
	MBeneficiarymapping identityEditDTOToMBeneficiarymapping(IdentityEditDTO dto);

	@Mapping(source = "dto.currentAddress.addrLine1", target = "currAddrLine1")
	@Mapping(source = "dto.currentAddress.addrLine2", target = "currAddrLine2")
	@Mapping(source = "dto.currentAddress.addrLine3", target = "currAddrLine3")
	@Mapping(source = "dto.currentAddress.countryId", target = "currCountryId")
	@Mapping(source = "dto.currentAddress.country", target = "currCountry")
	@Mapping(source = "dto.currentAddress.stateId", target = "currStateId")
	@Mapping(source = "dto.currentAddress.state", target = "currState")
	@Mapping(source = "dto.currentAddress.districtId", target = "currDistrictId")
	@Mapping(source = "dto.currentAddress.district", target = "currDistrict")
	@Mapping(source = "dto.currentAddress.subDistrictId", target = "currSubDistrictId")
	@Mapping(source = "dto.currentAddress.subDistrict", target = "currSubDistrict")
	@Mapping(source = "dto.currentAddress.villageId", target = "currVillageId")
	@Mapping(source = "dto.currentAddress.village", target = "currVillage")
	@Mapping(source = "dto.currentAddress.addressValue", target = "currAddressValue")
	@Mapping(source = "dto.currentAddress.pinCode", target = "currPinCode")
	@Mapping(source = "dto.emergencyAddress.addrLine1", target = "emerAddrLine1")
	@Mapping(source = "dto.emergencyAddress.addrLine2", target = "emerAddrLine2")
	@Mapping(source = "dto.emergencyAddress.addrLine3", target = "emerAddrLine3")
	@Mapping(source = "dto.emergencyAddress.countryId", target = "emerCountryId")
	@Mapping(source = "dto.emergencyAddress.country", target = "emerCountry")
	@Mapping(source = "dto.emergencyAddress.stateId", target = "emerStateId")
	@Mapping(source = "dto.emergencyAddress.state", target = "emerState")
	@Mapping(source = "dto.emergencyAddress.districtId", target = "emerDistrictId")
	@Mapping(source = "dto.emergencyAddress.district", target = "emerDistrict")
	@Mapping(source = "dto.emergencyAddress.subDistrictId", target = "emerSubDistrictId")
	@Mapping(source = "dto.emergencyAddress.subDistrict", target = "emerSubDistrict")
	@Mapping(source = "dto.emergencyAddress.villageId", target = "emerVillageId")
	@Mapping(source = "dto.emergencyAddress.village", target = "emerVillage")
	@Mapping(source = "dto.emergencyAddress.addressValue", target = "emerAddressValue")
	@Mapping(source = "dto.emergencyAddress.pinCode", target = "emerPinCode")
	@Mapping(source = "dto.permanentAddress.addrLine1", target = "permAddrLine1")
	@Mapping(source = "dto.permanentAddress.addrLine2", target = "permAddrLine2")
	@Mapping(source = "dto.permanentAddress.addrLine3", target = "permAddrLine3")
	@Mapping(source = "dto.permanentAddress.countryId", target = "permCountryId")
	@Mapping(source = "dto.permanentAddress.country", target = "permCountry")
	@Mapping(source = "dto.permanentAddress.stateId", target = "permStateId")
	@Mapping(source = "dto.permanentAddress.state", target = "permState")
	@Mapping(source = "dto.permanentAddress.districtId", target = "permDistrictId")
	@Mapping(source = "dto.permanentAddress.district", target = "permDistrict")
	@Mapping(source = "dto.permanentAddress.subDistrictId", target = "permSubDistrictId")
	@Mapping(source = "dto.permanentAddress.subDistrict", target = "permSubDistrict")
	@Mapping(source = "dto.permanentAddress.villageId", target = "permVillageId")
	@Mapping(source = "dto.permanentAddress.village", target = "permVillage")
	@Mapping(source = "dto.permanentAddress.addressValue", target = "permAddressValue")
	@Mapping(source = "dto.permanentAddress.pinCode", target = "permPinCode")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.eventTypeDate", target = "createdDate")

	
	@Mapping(source = "dto.permanentAddress.zoneID", target = "permZoneID")
	@Mapping(source = "dto.permanentAddress.zoneName", target = "permZone")
	@Mapping(source = "dto.permanentAddress.parkingPlaceID", target = "permAreaId")
	@Mapping(source = "dto.permanentAddress.parkingPlaceName", target = "permArea")
	@Mapping(source = "dto.permanentAddress.servicePointID", target = "permServicePointId")
	@Mapping(source = "dto.permanentAddress.servicePointName", target = "permServicePoint")
	@Mapping(source = "dto.permanentAddress.habitation", target = "permHabitation")

	@Mapping(source = "dto.currentAddress.zoneID", target = "currZoneID")
	@Mapping(source = "dto.currentAddress.zoneName", target = "currZone")
	@Mapping(source = "dto.currentAddress.parkingPlaceID", target = "currAreaId")
	@Mapping(source = "dto.currentAddress.parkingPlaceName", target = "currArea")
	@Mapping(source = "dto.currentAddress.servicePointID", target = "currServicePointId")
	@Mapping(source = "dto.currentAddress.servicePointName", target = "currServicePoint")
	@Mapping(source = "dto.currentAddress.habitation", target = "currHabitation")

	@Mapping(source = "dto.emergencyAddress.zoneID", target = "emerZoneID")
	@Mapping(source = "dto.emergencyAddress.zoneName", target = "emerZone")
	@Mapping(source = "dto.emergencyAddress.parkingPlaceID", target = "emerAreaId")
	@Mapping(source = "dto.emergencyAddress.parkingPlaceName", target = "emerArea")
	@Mapping(source = "dto.emergencyAddress.servicePointID", target = "emerServicePointId")
	@Mapping(source = "dto.emergencyAddress.servicePointName", target = "emerServicePoint")
	@Mapping(source = "dto.emergencyAddress.habitation", target = "emerHabitation")


	MBeneficiaryaddress identityEditDTOToMBeneficiaryaddress(IdentityEditDTO dto);

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
	@Mapping(source = "dto.eventTypeDate", target = "createdDate")
	MBeneficiaryconsent identityEditDTOToDefaultMBeneficiaryconsent(IdentityEditDTO dto, Boolean defaultYes,
			Boolean defaultNo);

	@Mapping(source = "dto.contact.preferredPhoneNum", target = "preferredPhoneNum")
	@Mapping(source = "dto.contact.preferredPhoneTyp", target = "preferredPhoneTyp")
	@Mapping(source = "dto.contact.preferredSMSPhoneNum", target = "preferredSMSPhoneNum")
	@Mapping(source = "dto.contact.preferredSMSPhoneTyp", target = "preferredSMSPhoneTyp")
	@Mapping(source = "dto.contact.emergencyContactNum", target = "emergencyContactNum")
	@Mapping(source = "dto.contact.phoneNum1", target = "phoneNum1")
	@Mapping(source = "dto.contact.phoneTyp1", target = "phoneTyp1")
	@Mapping(source = "dto.contact.phoneNum2", target = "phoneNum2")
	@Mapping(source = "dto.contact.phoneTyp2", target = "phoneTyp2")
	@Mapping(source = "dto.contact.phoneNum3", target = "phoneNum3")
	@Mapping(source = "dto.contact.phoneTyp3", target = "phoneTyp3")
	@Mapping(source = "dto.contact.phoneNum4", target = "phoneNum4")
	@Mapping(source = "dto.contact.phoneTyp4", target = "phoneTyp4")
	@Mapping(source = "dto.contact.phoneNum5", target = "phoneNum5")
	@Mapping(source = "dto.contact.phoneTyp5", target = "phoneTyp5")
	@Mapping(source = "dto.preferredEmailId", target = "emailId")
	@Mapping(source = "agentName", target = "createdBy")
	@Mapping(source = "eventTypeDate", target = "createdDate")

	MBeneficiarycontact identityEdiDTOToMBeneficiarycontact(IdentityEditDTO dto);

//	@Mapping(source = "dto.areaId", target = "areaId")
//	@Mapping(source = "dto.beneficiaryRegId", target = "beneficiaryRegID")
//	@Mapping(source = "dto.community", target = "community")
//	@Mapping(source = "dto.communityId", target = "communityId")
//	@Mapping(source = "dto.dob", target = "dob")
//	@Mapping(source = "dto.education", target = "education")
//	@Mapping(source = "dto.educationId", target = "educationId")
//
//	@Mapping(source = "dto.healthCareWorkerId", target = "healthCareWorkerId")
//	@Mapping(source = "dto.healthCareWorker", target = "healthCareWorker")
//	@Mapping(source = "dto.fatherName", target = "fatherName")
//	@Mapping(source = "dto.motherName", target = "motherName")
//	@Mapping(source = "dto.firstName", target = "firstName")
//	@Mapping(source = "dto.gender", target = "gender")
//	@Mapping(source = "dto.genderId", target = "genderId")
//	@Mapping(source = "dto.incomeStatusId", target = "incomeStatusId")
//	@Mapping(source = "dto.incomeStatus", target = "incomeStatus")
//	@Mapping(source = "dto.monthlyFamilyIncome", target = "monthlyFamilyIncome")
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
//	@Mapping(source = "dto.eventTypeDate", target = "createdDate")
//	@Mapping(target = "isHIVPositive", expression = "java(MBeneficiarydetail.setIsHIVPositive(dto.getIsHIVPositive()))")
//	// Start MMU specific code
//	@Mapping(target = "ageAtMarriage", expression = "java(MBeneficiarydetail.getAgeAtMarriageCalc(dto.getDob(), dto.getMarriageDate(), "
//			+ "dto.getAgeAtMarriage()))")
//	@Mapping(target = "marriageDate", expression = "java(MBeneficiarydetail.getMarriageDateCalc(dto.getDob(), dto.getMarriageDate(), "
//			+ "dto.getAgeAtMarriage()))")
//	
//	MBeneficiarydetail identityEditDTOToMBeneficiarydetail(IdentityEditDTO dto);

	@Mapping(source = "benFamilyDTO.isEmergencyContact", target = "isEmergencyContact")
	@Mapping(source = "benFamilyDTO.relationshipToSelf", target = "relationshipToSelf")
	@Mapping(source = "benFamilyDTO.associatedBenRegId", target = "associatedBenRegId")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	MBeneficiaryfamilymapping identityEditDTOToMBeneficiaryfamilymapping(BenFamilyDTO benFamilyDTO, String createdBy,
			Timestamp createdDate);

	List<MBeneficiaryfamilymapping> identityEditDTOListToMBeneficiaryfamilymappingList(List<BenFamilyDTO> list);

	@Mapping(source = "identity.identityNo", target = "identityNo")
	@Mapping(source = "identity.identityName", target = "identityName")
	@Mapping(source = "identity.identityType", target = "identityType")
	@Mapping(source = "identity.issueDate", target = "issueDate")
	@Mapping(source = "identity.expiryDate", target = "expiryDate")
	@Mapping(source = "identity.isVerified", target = "isVerified")
	@Mapping(source = "identity.identityFilePath", target = "identityFilePath")
	@Mapping(source = "createdBy", target = "createdBy")
	@Mapping(source = "createdDate", target = "createdDate")
	MBeneficiaryidentity identityToMBeneficiaryidentity(Identity identity, String createdBy, Timestamp createdDate);

	List<MBeneficiaryidentity> identityEditDTOListToMBeneficiaryidentityList(List<Identity> list);

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
	@Mapping(source = "dto.eventTypeDate", target = "createdDate")
	MBeneficiaryservicemapping identityEditDTOToMBeneficiaryservicemapping(IdentityEditDTO dto);

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
	@Mapping(source = "preferredLanguage", target = "preferredLanguage")
	@Mapping(source = "preferredLanguageId", target = "preferredLanguageId")
	@Mapping(source = "religionId", target = "religionId")
	@Mapping(source = "religion", target = "religion")
	@Mapping(source = "remarks", target = "remarks")
	@Mapping(source = "servicePointId", target = "servicePointId")
	@Mapping(source = "sourceOfInfo", target = "sourceOfInfo")
	@Mapping(source = "spouseName", target = "spouseName")
	@Mapping(source = "status", target = "status")
	@Mapping(source = "title", target = "title")
	@Mapping(source = "zoneId", target = "zoneId")
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
	@Mapping(source = "identityNo", target = "identityNo")
	@Mapping(source = "identityType", target = "identityType")
	@Mapping(source = "issueDate", target = "issueDate")
	@Mapping(source = "isVerified", target = "isVerified")
	@Mapping(source = "lastModDate", target = "lastModDate")
	@Mapping(source = "modifiedBy", target = "modifiedBy")
	BenIdentityDTO mBeneficiaryidentityToBenIdentityEditDTO(MBeneficiaryidentity identity);

	List<BenIdentityDTO> mBeneficiaryidentityListToBenIdentityEditDTOList(List<MBeneficiaryidentity> identity);

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
	@Mapping(source = "dto.eventTypeDate", target = "createdDate")
	@Mapping(source = "dto.benAccountID", target = "benAccountID")
	MBeneficiaryAccount identityEditDTOToMBeneficiaryAccount(IdentityEditDTO dto);

	List<MBeneficiaryAccount> identityEditDTOToMBeneficiaryAccount(List<IdentityEditDTO> dto);

	@Mapping(source = "dto.benImage", target = "benImage")
	@Mapping(source = "dto.agentName", target = "createdBy")
	@Mapping(source = "dto.eventTypeDate", target = "createdDate")
	@Mapping(source = "dto.benImageId", target = "benImageId")
	MBeneficiaryImage identityEditDTOToMBeneficiaryImage(IdentityEditDTO dto);

	List<MBeneficiaryImage> identityEditDTOToMBeneficiaryImage(List<IdentityEditDTO> dto);

}
