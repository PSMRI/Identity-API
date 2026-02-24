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
package com.iemr.common.identity.domain.identity;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import com.google.gson.annotations.Expose;

import lombok.AllArgsConstructor;
import lombok.Data;

@Entity
@Table(name = "i_beneficiarydetails")
@NamedQuery(name = "MBeneficiarydetail.findAll", query = "SELECT m FROM MBeneficiarydetail m order by beneficiaryDetailsId asc")
@Data
@AllArgsConstructor
public class MBeneficiarydetail implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final int START_YEAR = 1970;
	private static final String POSITIVE = "yes";
	private static final String NEGETIVE = "no";
	private static final String ND = "";
	private static final int POSITIVE_INT = 1;
	private static final int NEGETIVE_INT = 2;
	private static final int ND_INT = 3;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger beneficiaryDetailsId;

	private Integer areaId;

	private BigInteger beneficiaryRegID;

	@Column(length = 45)
	private String community;

	@Column(nullable = false, length = 50, updatable = false)
	private String createdBy;

	@Column(name = "CreatedDate",updatable = false)
	private Timestamp createdDate;

	private Boolean deleted = false;

	private Timestamp dob;

	@Column(length = 45)
	private String education;
    
	@Column(name = "EmergencyRegistration")
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
	
	@Column(name = "faceEmbedding")
	private String faceEmbedding;


	@Column(length = 20)
	private String maritalStatus;

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
	private BigInteger religionId;

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
	private String spouseName;

	@Column(length = 10)
	private String status;

	@Column(length = 10)
	private String title;

	private Integer zoneId;

	@OneToOne(mappedBy = "mBeneficiarydetail")
	private MBeneficiarymapping mBeneficiarymapping;

	@Column(name = "GenderID")
	private Integer genderId;

	@Column(name = "MaritalStatusID")
	private Integer maritalStatusId;

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
	@Column(name = "HeadofFamily_RelationID")
	private Integer headOfFamily_RelationID;
	@Expose
	@Column(name = "familyid")
	private String familyId;

	@Column(name = "others")
	private String other;

	@Expose
	@Column(name = "HeadofFamily_Relation")
	private String headOfFamily_Relation;
	// new column added for data sync
	// 17-06-2018
	@Expose
	@Column(name = "vanID", updatable = false)
	private Integer vanID;
	@Expose
	@Column(name = "parkingPlaceID", updatable = false)
	private Integer parkingPlaceID;

	@Expose
	@Column(name = "VanSerialNo", updatable = false)
	private BigInteger vanSerialNo;
	
	@Column(name = "MonthlyFamilyIncome")
	private String monthlyFamilyIncome;
	
	@Column(name = "ExtraFields")
	private String otherFields;
	
//	@Column(columnDefinition = "json")
//	private JsonElement otherFields;

	public MBeneficiarydetail() {

	}

	public MBeneficiarydetail(BigInteger beneficiaryDetailsId, String firstName, String lastName, String middleName,
			String fatherName, String spouseName) {

		this.beneficiaryDetailsId = beneficiaryDetailsId;
		this.firstName = firstName;
		this.lastName = lastName;
		this.middleName = middleName;
		this.fatherName = fatherName;
		this.spouseName = spouseName;
	}

	public static Timestamp getMarriageDateCalc(Timestamp dob, Timestamp marriageDate, Integer ageAtMarriage) {
		Timestamp marriageDateCalc = null;
		if (marriageDate != null) {
			marriageDateCalc = marriageDate;
		} else if (dob != null && ageAtMarriage != null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(dob);
			cal.set(Calendar.YEAR, cal.get(Calendar.YEAR) + ageAtMarriage);
			marriageDateCalc = new Timestamp(cal.getTime().getTime());
		}
		return marriageDateCalc;
	}

	public static Integer getAgeAtMarriageCalc(Timestamp dob, Timestamp marriageDate, Integer ageAtMarriage) {
		Integer ageAtMarriageCalc = null;
		if (ageAtMarriage != null) {
			ageAtMarriageCalc = ageAtMarriage;
		} else if (dob != null && marriageDate != null) {
			Calendar dobCal = Calendar.getInstance();
			Calendar marriageCal = Calendar.getInstance();
			dobCal.setTime(dob);
			marriageCal.setTime(marriageDate);
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date(marriageCal.getTime().getTime() - dobCal.getTime().getTime()));
			ageAtMarriageCalc = cal.get(Calendar.YEAR) - START_YEAR;
		}
		return ageAtMarriageCalc;
	}

	public static Integer calculateAge(Timestamp dob) {
		Integer beneficiaryAge = null;
		if (dob != null) {
			Calendar dobCal = Calendar.getInstance();
			Calendar currcal = Calendar.getInstance();
			dobCal.setTime(new Date(currcal.getTime().getTime() - dob.getTime()));
			beneficiaryAge = dobCal.get(Calendar.YEAR) - START_YEAR;
		}
		return beneficiaryAge;
	}

	public void setIsHIVPositive(Integer isHivPositive) {
		this.isHIVPositive = isHivPositive;
	}

	public Integer getIsHIVPositive() {
		return this.isHIVPositive;
	}

	public static Integer setIsHIVPositive(String status) {
		Integer isHIVPositive = ND_INT;
		if (status != null) {
			switch (status.toLowerCase()) {
			case POSITIVE:
				isHIVPositive = POSITIVE_INT;
				break;
			case NEGETIVE:
				isHIVPositive = NEGETIVE_INT;
				break;
			}
		}
		return isHIVPositive;
	}

	public static String getIsHIVPositive(Integer status) {
		String isHIVPositive = ND;
		if (status != null) {
			switch (status) {
			case POSITIVE_INT:
				isHIVPositive = POSITIVE;
				break;
			case NEGETIVE_INT:
				isHIVPositive = NEGETIVE;
				break;
			}
		}
		return isHIVPositive;
	}

}
