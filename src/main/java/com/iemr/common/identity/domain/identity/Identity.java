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

import lombok.Data;

@Data
public class Identity
{
	private String identityNo;
	private Integer identityNameId;
	private String identityName;
	private Integer identityTypeId;
	private String identityType;
	private Timestamp issueDate;
	private Timestamp expiryDate;
	private Boolean isVerified;
	private String identityFilePath;
	private String createdBy;
	private BigInteger benIdentityId;
	private Boolean deleted;
}
