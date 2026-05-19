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
package com.iemr.common.identity.data.rmnch;

import java.math.BigInteger;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import com.google.gson.annotations.Expose;

import lombok.Data;

@Entity
@Table(name = "i_beneficiarydetails")
@Data
public class RMNCHMBeneficiarydetail {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger beneficiaryDetailsId;

	private Integer areaId;

	@Column(name = "beneficiaryRegID")
	private BigInteger BenRegId;

	@Column(length = 45)
	private String community;

	@Column(nullable = false, length = 50, updatable = false)
	private String createdBy;

	@Column(name = "CreatedDate")
	private Timestamp createdDate;

	@Column(name = "dob")
	private Timestamp dob;

	private Boolean deleted = false;

	@Column(length = 45)
	private String education;

	private Boolean emergencyRegistration;

	@Column(length = 50)
	private String fatherName;

	@Column(length = 20)
	private String firstName;

	@Column(length = 50)
	private String motherName;

	private String gender;

	@Column(name = "incomeStatusId")
	private Integer incomeStatusId;

	@Column(length = 45)
	private String incomeStatus;

	@Column(name = "LastModDate", insertable = false, updatable = false)
	private Timestamp lastModDate;

	@Column(length = 20)
	private String lastName;

	@Column(length = 20)
	private String maritalstatus;

	@Column(name = "marriageDate")
	private Timestamp marriageDate;

	@Transient
	private Integer ageAtMarriage;

	@Column(name = "MiddleName", length = 20)
	private String middleName;

	@Column(length = 50)
	private String modifiedBy;

	private Integer occupationId;

	@Column(length = 45)
	private String occupation;

	private Integer phcId;

	@Column(length = 30)
	private String placeOfWork;

	private String literacyStatus;

	private Integer preferredLanguageId;

	@Column(length = 45)
	private String preferredLanguage;

	@Column(nullable = false, length = 4)
	private String processed = "N";

	@Column(length = 45)
	private BigInteger religionID;

	@Column(length = 45)
	private String religion;

	@Column(length = 300)
	private String remarks;

	private Boolean reserved;

	private Integer reservedById;

	@Column(length = 45)
	private String reservedFor;

	private Timestamp reservedOn;

	private BigInteger servicePointId;

	@Column(length = 45)
	private String sourceOfInfo;

	@Column(length = 50)
	private String spousename;

	@Column(length = 10)
	private String status;

	@Column(length = 10)
	private String title;

	private Integer zoneId;

	@Column(name = "GenderID")
	private Integer genderId;

	@Column(name = "MaritalStatusID")
	private Integer maritalstatusId;

	@Column(name = "TitleID")
	private Integer titleId;

	@Column(name = "CommunityID")
	private Integer communityId;

	@Column(name = "EducationID")
	private Integer educationId;

	@Column(name = "SexualOrientationID")
	private Integer sexualOrientationID;

	@Column(name = "SexualOrientationType")
	private String sexualOrientationType;

	@Column(name = "HIVStatus")
	private Integer isHIVPositive;

	@Column(name = "HealthCareWorkerId")
	private Integer healthCareWorkerId;

	private String healthCareWorker;

	@Expose
	@Column(name = "vanID", updatable = false)
	private Integer VanID;
	@Expose
	@Column(name = "parkingPlaceID", updatable = false)
	private Integer parkingPlaceID;

	@Expose
	@Column(name = "VanSerialNo", updatable = false)
	private BigInteger id;

	@Expose
	@Column(name = "SyncedBy")
	private String SyncedBy;

	@Expose
	@Column(name = "SyncedDate")
	private Timestamp SyncedDate;

	@Transient
	private BigInteger benficieryid;

	@Expose
	@Transient
	private Integer ProviderServiceMapID;
}