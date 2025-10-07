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

//@Data
public class BenIdImportDTO {
	private BigInteger benRegId;
	private BigInteger beneficiaryId;
	private String createdBy;
	private Timestamp createdDate;
	private BigInteger vanID;

	public BigInteger getBenRegId() {
		return benRegId;
	}

	public void setBenRegId(BigInteger benRegId) {
		this.benRegId = benRegId;
	}

	public BigInteger getBeneficiaryId() {
		return beneficiaryId;
	}

	public void setBeneficiaryId(BigInteger beneficiaryId) {
		this.beneficiaryId = beneficiaryId;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public void setVanID(BigInteger vanID) {
		this.vanID = vanID;
	}

	public BigInteger getVanID() {
		return vanID;
	}	
}
