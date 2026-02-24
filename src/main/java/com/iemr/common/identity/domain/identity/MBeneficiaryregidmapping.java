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
 * The persistent class for the m_beneficiaryregid database table.
 * 
 */
@Entity
@Table(name = "m_beneficiaryregidmapping")
@NamedQuery(name = "MBeneficiaryregidmapping.findAll", query = "SELECT m FROM MBeneficiaryregidmapping m")
@Data
public class MBeneficiaryregidmapping implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger benRegId;

	@Column(nullable = false)
	private BigInteger beneficiaryID;

	@Column(length = 30)
	private String createdBy;

	@Column(name = "CreatedDate",updatable = false)
	private Timestamp createdDate;

	private Boolean deleted = false;

	private Boolean provisioned;

	private Integer providerServiceMapID;

	private Boolean reserved;

	private Integer reservedById;

	@Column(length = 45)
	private String reservedFor;

	private Timestamp reservedOn;

	@Column(name = "VehicalNo")
	private String vehicalNo;


	private String syncedBy;

	private Timestamp syncedDate;

	private String reservedForChange;

	@Expose
	@Column(name = "vanID")
	private Integer vanID;
	@Expose
	@Column(name = "parkingPlaceID")
	private Integer parkingPlaceID;

	@Expose
	@Column(name = "VanSerialNo")
	private BigInteger vanSerialNo;

}