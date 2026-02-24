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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;

import com.google.gson.annotations.Expose;

import lombok.Data;

/**
 * The persistent class for the m_beneficiaryconsent database table.
 * 
 */
@Entity
@Table(name = "i_beneficiaryconsent")
@NamedQuery(name = "MBeneficiaryconsent.findAll", query = "SELECT m FROM MBeneficiaryconsent m")
@Data
public class MBeneficiaryconsent implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger benConsentID;

	@Column(nullable = false, length = 50)
	private String createdBy;

	@Column(name = "CreatedDate",updatable = false)
	private Timestamp createdDate;

	private Boolean deleted = false;

	private Timestamp lastModDate;

	@Column(length = 50)
	private String modifiedBy;

	@Column(nullable = false, length = 4)
	private String processed = "N";

	private Boolean reserved;

	private Integer reservedById;

	@Column(length = 45)
	private String reservedFor;

	private Timestamp reservedOn;

	private Boolean shareAnonymousWithGovt;

	private Boolean shareAnonymousWithMedicalCommunity;

	private Boolean shareAnonymousWithNGO;

	private Boolean shareMedicalDetailsForMedicalStudy;

	private Boolean shareMedicalDetailsWithDoctor;

	private Boolean shareMedicalDetailsWithFamily;

	private Boolean shareMedicalDetailsWithFriends;

	private Boolean shareMedicalDetailsWithGovt;

	private Boolean shareMedicalDetailsWithNGO;

	private Boolean shareMedicalDetailsWithSpouse;

	private Boolean sharePersonalDetailsForMedicalStudy;

	private Boolean sharePersonalDetailsWithDoctor;

	private Boolean sharePersonalDetailsWithFamily;

	private Boolean sharePersonalDetailsWithFriends;

	private Boolean sharePersonalDetailsWithGovt;

	private Boolean sharePersonalDetailsWithNGO;

	private Boolean sharePersonalDetailsWithSpouse;

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

	// END OF new column added for data sync

	@Expose
	@Column(name = "BenConsent", updatable = false)
	private Boolean beneficiaryConsent;

}