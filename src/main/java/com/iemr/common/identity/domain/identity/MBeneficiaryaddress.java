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
 * The persistent class for the m_beneficiaryaddress database table.
 * 
 */
@Entity
@Table(name = "i_beneficiaryaddress")
@NamedQuery(name = "MBeneficiaryaddress.findAll", query = "SELECT m FROM MBeneficiaryaddress m")
@Data
public class MBeneficiaryaddress implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger benAddressID;

	@Column(nullable = false, length = 50)
	private String createdBy;

	@Column(name = "CreatedDate",updatable = false)
	private Timestamp createdDate;

	@Column(length = 50)
	private String currAddrLine1;

	@Column(length = 50)
	private String currAddrLine2;

	@Column(length = 50)
	private String currAddrLine3;

	@Column(length = 15)
	private String currCountry;

	private Integer currCountryId;

	@Column(length = 50)
	private String currDistrict;

	private Integer currDistrictId;

	@Column(length = 250)
	private String currAddressValue;

	private String currPinCode;

	@Column(length = 30)
	private String currState;

	private Integer currStateId;

	@Column(length = 50)
	private String currSubDistrict;

	private Integer currSubDistrictId;

	@Column(length = 100)
	private String currVillage;

	private String currHabitation;

	private Integer currVillageId;

	private Boolean deleted = false;

	@Column(length = 50)
	private String emerAddrLine1;

	@Column(length = 50)
	private String emerAddrLine2;

	@Column(length = 50)
	private String emerAddrLine3;

	@Column(length = 15)
	private String emerCountry;

	private Integer emerCountryId;

	@Column(length = 50)
	private String emerDistrict;

	private Integer emerDistrictId;

	@Column(length = 250)
	private String emerAddressValue;

	private String emerPinCode;

	@Column(length = 30)
	private String emerState;

	private Integer emerStateId;

	@Column(length = 50)
	private String emerSubDistrict;

	private Integer emerSubDistrictId;

	@Column(length = 100)
	private String emerVillage;

	private String emerHabitation;

	private Integer emerVillageId;

	private Timestamp lastModDate;

	@Column(length = 50)
	private String modifiedBy;

	@Column(length = 50)
	private String permAddrLine1;

	@Column(length = 50)
	private String permAddrLine2;

	@Column(length = 50)
	private String permAddrLine3;

	@Column(length = 250)
	private String permAddressValue;

	@Column(length = 15)
	private String permCountry;

	private Integer permCountryId;

	@Column(length = 50)
	private String permDistrict;

	private Integer permDistrictId;

	private String permPinCode;

	@Column(length = 30)
	private String permState;

	private Integer permStateId;

	@Column(length = 50)
	private String permSubDistrict;

	private Integer permSubDistrictId;

	@Column(length = 100)
	private String permVillage;

	private String permHabitation;

	private Integer permVillageId;

	/*
	 * New columns added for MMU integration 09-04-2018
	 */
	@Column(name = "PermZoneID")
	private Integer permZoneID;
	@Column(name = "PermZone")
	private String permZone;
	@Column(name = "PermAreaId")
	private Integer permAreaId;
	@Column(name = "PermArea")
	private String permArea;
	@Column(name = "PermServicePointId")
	private Integer permServicePointId;
	@Column(name = "PermServicePoint")
	private String permServicePoint;

	@Column(name = "CurrZoneID")
	private Integer currZoneID;
	@Column(name = "CurrZone")
	private String currZone;
	@Column(name = "CurrAreaId")
	private Integer currAreaId;
	@Column(name = "CurrArea")
	private String currArea;
	@Column(name = "CurrServicePointId")
	private Integer currServicePointId;
	@Column(name = "CurrServicePoint")
	private String currServicePoint;

	@Column(name = "EmerZoneID")
	private Integer emerZoneID;
	@Column(name = "EmerZone")
	private String emerZone;
	@Column(name = "EmerAreaId")
	private Integer emerAreaId;
	@Column(name = "EmerArea")
	private String emerArea;
	@Column(name = "EmerServicePointId")
	private Integer emerServicePointId;
	@Column(name = "EmerServicePoint")
	private String emerServicePoint;

	// End

	@Column(nullable = false, length = 4)
	private String processed = "N";

	private Boolean reserved;

	private Integer reservedById;

	@Column(length = 45)
	private String reservedFor;

	private Timestamp reservedOn;

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

	public MBeneficiaryaddress setCurrentAddress(Address address) {
		this.currAddrLine1 = address.getAddrLine1();
		this.currAddrLine2 = address.getAddrLine2();
		this.currAddrLine3 = address.getAddrLine3();
		this.currCountryId = address.getCountryId();
		this.currCountry = address.getCountry();
		this.currStateId = address.getStateId();
		this.currState = address.getState();
		this.currDistrictId = address.getDistrictId();
		this.currDistrict = address.getDistrict();
		this.currSubDistrictId = address.getSubDistrictId();
		this.currSubDistrict = address.getSubDistrict();
		this.currVillageId = address.getVillageId();
		this.currVillage = address.getVillage();
		this.currAddressValue = address.getAddressValue();
		this.currPinCode = address.getPinCode();

		return this;
	}

	public MBeneficiaryaddress setPermanentAddress(Address address) {
		this.permAddrLine1 = address.getAddrLine1();
		this.permAddrLine2 = address.getAddrLine2();
		this.permAddrLine3 = address.getAddrLine3();
		this.permCountryId = address.getCountryId();
		this.permCountry = address.getCountry();
		this.permStateId = address.getStateId();
		this.permState = address.getState();
		this.permDistrictId = address.getDistrictId();
		this.permDistrict = address.getDistrict();
		this.permSubDistrictId = address.getSubDistrictId();
		this.permSubDistrict = address.getSubDistrict();
		this.permVillageId = address.getVillageId();
		this.permVillage = address.getVillage();
		this.permAddressValue = address.getAddressValue();
		this.permPinCode = address.getPinCode();

		return this;
	}

	public MBeneficiaryaddress setEmergencyAddress(Address address) {
		this.emerAddrLine1 = address.getAddrLine1();
		this.emerAddrLine2 = address.getAddrLine2();
		this.emerAddrLine3 = address.getAddrLine3();
		this.emerCountryId = address.getCountryId();
		this.emerCountry = address.getCountry();
		this.emerStateId = address.getStateId();
		this.emerState = address.getState();
		this.emerDistrictId = address.getDistrictId();
		this.emerDistrict = address.getDistrict();
		this.emerSubDistrictId = address.getSubDistrictId();
		this.emerSubDistrict = address.getSubDistrict();
		this.emerVillageId = address.getVillageId();
		this.emerVillage = address.getVillage();
		this.emerAddressValue = address.getAddressValue();
		this.emerPinCode = address.getPinCode();

		return this;
	}
}