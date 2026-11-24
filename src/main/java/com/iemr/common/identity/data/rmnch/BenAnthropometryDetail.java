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

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.Data;

/**
 * Mirrors FLW-API's BenAnthropometryDetail (same db_iemr.t_phy_anthropometry table), so syncDataToAmrit can write
 * registration-time values that FLW-API getBeneficiaryData reads back.
 */
@Entity
@Table(name = "t_phy_anthropometry", catalog = "db_iemr")
@Data
public class BenAnthropometryDetail {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID")
	private Long id;

	@Column(name = "BeneficiaryRegID")
	private Long beneficiaryRegID;

	@Column(name = "BenVisitID")
	private Long benVisitID;

	@Column(name = "ProviderServiceMapID")
	private Integer providerServiceMapID;

	@Column(name = "VisitCode")
	private Long visitCode;

	@Column(name = "Weight_Kg")
	private Double weightKg;

	@Column(name = "Height_cm")
	private Double heightCm;

	@Column(name = "BMI")
	private Double bmi;

	@Column(name = "Deleted", insertable = false, updatable = true)
	private Boolean deleted;

	@Column(name = "Processed", insertable = false, updatable = true)
	private String processed;

	@Column(name = "CreatedBy")
	private String createdBy;

	@Column(name = "CreatedDate", insertable = false, updatable = false)
	private Timestamp createdDate;

	@Column(name = "ModifiedBy")
	private String modifiedBy;

	@Column(name = "LastModDate", insertable = false, updatable = false)
	private Timestamp lastModDate;

	@Column(name = "VanSerialNo")
	private Long vanSerialNo;

	@Column(name = "VanID")
	private Integer vanID;

	@Column(name = "ParkingPlaceID")
	private Integer parkingPlaceID;

	@Column(name = "SyncedBy")
	private String syncedBy;

	@Column(name = "SyncedDate")
	private Timestamp syncedDate;
}
