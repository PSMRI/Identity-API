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
package com.iemr.common.identity.dto;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import com.iemr.common.identity.domain.identity.Address;
import com.iemr.common.identity.domain.identity.Contact;
import com.iemr.common.identity.domain.identity.Identity;

import lombok.Data;

@Data
public class IdentityEditDTO {

	private String eventTypeName; // created, reserved, registered, modified, deleted
	private Timestamp eventTypeDate;
	private Integer agentId;
	private String agentName;
	private Integer agentPSMapId; // ProviderServiceMapId
	private String agentComment;
	private Integer serviceId;
	private String serviceName;
	private Integer serviceProviderId;
	private String serviceProviderName;
	private Integer stateId;
	private String stateName;
	private Integer providerServiceMapId;
	private BigInteger beneficaryId;
	private BigInteger beneficiaryRegId;

	// self details
	private Boolean changeInSelfDetails = false;
	private String firstName;
	private String middleName;
	private String lastName;
	private String fatherName;
	private String motherName;
	private String spouseName;
	private Integer maritalStatusId;
	private String maritalStatus;
	private Timestamp dob;
	private Integer genderId;
	private String gender;
	private Integer titleId;
	private String title;
	private String literacyStatus;

	// self address
	private Boolean changeInAddress = false;
	private Address currentAddress;
	private Address permanentAddress;
	private Address emergencyAddress;
	private Boolean isPermAddrSameAsCurrAddr;
	private Boolean isPermAddrSameAsEmerAddr;
	private Boolean isEmerAddrSameAsCurrAddr;
	private Boolean isEmerAddrSameAsPermAddr;
	private String addressType;

	// self contacts
	private Boolean changeInContacts = false;
	private Contact contact;
	private String preferredEmailId;

	// self identities
	private Boolean changeInIdentities = false;
	private List<Identity> identities;

	// self details
	private Boolean changeInOtherDetails = false;
	private Integer preferredLanguageId;
	private String preferredLanguage;
	private Integer communityId;
	private String community;
	private Integer educationId;
	private String education;
	private Integer incomeStatusId;
	private String incomeStatus;
	private Integer occupationId;
	private String occupationName;
	private Integer religionId;
	private String religion;
	private String placeOfWork;
	private Integer healthCareWorkerId;
	private String healthCareWorker;
	private String monthlyFamilyIncome;

	// family details
	private Boolean changeInFamilyDetails = false;
	List<BenFamilyDTO> benFamilyDTOs;
	private Boolean changeInAssociations = false;

	private Integer zoneId;
	private Integer areaId;
	private BigInteger servicePointId;
	private Integer phcId;
	private String remarks;
	private String sourceOfInfo;
	private Integer statusId;
	private String status;

	/*
	 * New columns added for MMU integration 11-04-2018
	 */
	private Boolean changeInBankDetails = false;
	private String bankName;
	private String branchName;
	private String ifscCode;
	private String accountNo;
	private BigInteger benAccountID;

	private Boolean changeInBenImage = false;
	private String benImage;
	private Long benImageId;

	private Timestamp marriageDate;
	private Integer ageAtMarriage;

	// Start 1097
	private Integer sexualOrientationID;
	private String sexualOrientationType;
	private String isHIVPositive;
	// End 1097

	private Integer vanID;
	private Integer parkingPlaceId;
	private boolean emergencyRegistration;
	private String otherFields;
}
