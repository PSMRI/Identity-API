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

import java.math.BigInteger;
import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.google.gson.annotations.Expose;

import lombok.Data;

@Entity
@Table(name = "i_beneficiaryimage")
@Data
public class MBeneficiaryImage {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "BenImageId")
	private BigInteger benImageId;
	@Column(name = "BenImage")
	private String benImage;
	@Column(name = "BenBiometric")
	private String benBiometric;
	@Column(name = "Deleted", insertable = false, updatable = true)
	private Boolean deleted = false;
	@Column(name = "Processed", insertable = false)
	private String processed;
	@Column(name = "CreatedBy", updatable = false)
	private String createdBy;
	@Column(name = "CreatedDate",updatable = false)
	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	private Timestamp createdDate;
	@Column(name = "Reserved")
	private Boolean reserved;
	@Column(name = "ReservedFor")
	private String reservedFor;
	@Column(name = "ReservedOn")
	private String reservedOn;
	@Column(name = "ReservedById")
	private Integer reservedById;
	@Column(name = "ModifiedBy")
	private String modifiedBy;
	@Column(name = "LastModDate")
	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	private Timestamp lastModDate;

	@Column(name = "VehicalNo")
	private String vehicalNo;
	@Column(name = "SyncedBy")
	private String syncedBy;
	@Column(name = "SyncedDate", unique = true, nullable = false)
	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	private Timestamp syncedDate;
	@Column(name = "ReservedForChange")
	private String reservedForChange;

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
	private Long vanSerialNo;

	// END OF new column added for data sync

}
