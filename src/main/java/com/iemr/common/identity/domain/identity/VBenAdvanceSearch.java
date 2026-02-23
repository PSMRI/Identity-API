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
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.google.gson.annotations.Expose;

@Entity
@Table(name = "v_benadvancesearch")
public class VBenAdvanceSearch {

	@Id
	@Expose
	@Column(name = "BenMapID")
	private BigInteger benMapID;
	@Expose
	@Column(name = "BenRegID")
	private BigInteger benRegID;
	@Expose
	@Column(name = "VanSerialNo")
	private BigInteger vanSerialNo;
	@Expose
	@Column(name = "VanID")
	private Integer vanID;
	@Expose
	@Column(name = "BenDetailsId")
	private BigInteger benDetailsID;
	@Expose
	@Column(name = "BenAddressId")
	private BigInteger benAddressID;
	@Expose
	@Column(name = "BenImageId")
	private BigInteger benImageID;
	@Expose
	@Column(name = "BenContactsId")
	private BigInteger benContactID;
	@Expose
	@Column(name = "BenConsentId")
	private BigInteger benConsentID;
	@Expose
	@Column(name = "BenAccountID")
	private BigInteger benAccountID;
	@Expose
	@Column(name = "FirstName")
	private String firstName;
	@Expose
	@Column(name = "MiddleName")
	private String middleName;
	@Expose
	@Column(name = "LastName")
	private String lastName;
	@Expose
	@Column(name = "GenderId")
	private Integer genderID;
	@Expose
	@Column(name = "CurrDistrictId")
	private Integer districtID;
	@Expose
	@Column(name = "CurrSubDistrictId")
	private Integer subDistrictID;
	@Expose
	@Column(name = "CurrVillageId")
	private Integer villageID;
	@Expose
	@Column(name = "CurrStateId")
	private Integer stateID;
	@Expose
	@Column(name = "fathername")
	private String fatherName;
	@Expose
	@Column(name = "DOB")
	private Timestamp dOB;

	@Expose
	@Column(name = "houseoldId")
	private Long houseHoldID;

	@Expose
	@Column(name = "guidelineid")
	private String guideLineID;

	@Expose
	@Column(name = "rchid")
	private String rchID;

	public String getRchID() {
		return rchID;
	}

	public void setRchID(String rchID) {
		this.rchID = rchID;
	}

	public Integer getVillageID() {
		return villageID;
	}

	public void setVillageID(Integer villageID) {
		this.villageID = villageID;
	}

	public String getGuideLineID() {
		return guideLineID;
	}

	public void setGuideLineID(String guideLineID) {
		this.guideLineID = guideLineID;
	}

	public Long getHouseHoldID() {
		return houseHoldID;
	}

	public void setHouseHoldID(Long houseHoldID) {
		this.houseHoldID = houseHoldID;
	}

	public VBenAdvanceSearch() {
		super();
	}

	public BigInteger getBenMapID() {
		return benMapID;
	}

	public void setBenMapID(BigInteger benMapID) {
		this.benMapID = benMapID;
	}

	public BigInteger getBenRegID() {
		return benRegID;
	}

	public void setBenRegID(BigInteger benRegID) {
		this.benRegID = benRegID;
	}

	public BigInteger getVanSerialNo() {
		return vanSerialNo;
	}

	public void setVanSerialNo(BigInteger vanSerialNo) {
		this.vanSerialNo = vanSerialNo;
	}

	public Integer getVanID() {
		return vanID;
	}

	public void setVanID(Integer vanID) {
		this.vanID = vanID;
	}

	public BigInteger getBenDetailsID() {
		return benDetailsID;
	}

	public void setBenDetailsID(BigInteger benDetailsID) {
		this.benDetailsID = benDetailsID;
	}

	public BigInteger getBenAddressID() {
		return benAddressID;
	}

	public void setBenAddressID(BigInteger benAddressID) {
		this.benAddressID = benAddressID;
	}

	public BigInteger getBenImageID() {
		return benImageID;
	}

	public void setBenImageID(BigInteger benImageID) {
		this.benImageID = benImageID;
	}

	public BigInteger getBenContactID() {
		return benContactID;
	}

	public void setBenContactID(BigInteger benContactID) {
		this.benContactID = benContactID;
	}

	public BigInteger getBenConsentID() {
		return benConsentID;
	}

	public void setBenConsentID(BigInteger benConsentID) {
		this.benConsentID = benConsentID;
	}

	public BigInteger getBenAccountID() {
		return benAccountID;
	}

	public void setBenAccountID(BigInteger benAccountID) {
		this.benAccountID = benAccountID;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Integer getGenderID() {
		return genderID;
	}

	public void setGenderID(Integer genderID) {
		this.genderID = genderID;
	}

	public Integer getDistrictID() {
		return districtID;
	}

	public void setDistrictID(Integer districtID) {
		this.districtID = districtID;
	}

	public Integer getSubDistrictID() {
		return subDistrictID;
	}

	public void setSubDistrictID(Integer subDistrictID) {
		this.subDistrictID = subDistrictID;
	}

	public Integer getStateID() {
		return stateID;
	}

	public void setStateID(Integer stateID) {
		this.stateID = stateID;
	}

	public String getFatherName() {
		return fatherName;
	}

	public void setFatherName(String fatherName) {
		this.fatherName = fatherName;
	}

	public Timestamp getdOB() {
		return dOB;
	}

	public void setdOB(Timestamp dOB) {
		this.dOB = dOB;
	}

}
