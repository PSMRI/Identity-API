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

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;

import lombok.Data;

@Entity
@Table(name="m_bensecurestack")
@NamedQuery(name="MBensecurestack.findAll", query="SELECT m FROM MBensecurestack m")
@Data
public class MBensecurestack {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(unique=true, nullable=false)
	private Integer benSecureStackId;
	
	private Integer benSecureStackSchemeId;
	
	@Column(nullable=false, length=50)
	private String benSecureStackSchemeName;
	
	@Column(nullable=false, length=128)
	private String benKey1;
	
	@Column(nullable=false, length=128)
	private String benKey2;
	
	@Column(nullable=false, length=128)
	private String benKey3;
	
	@Column(nullable=false, length=256)
	private String benKey4;
	
	@Column(nullable=false, length=256)
	private String benKey5;
	
	@Column(nullable=false, length=256)
	private String benKey6;
	
	@Column(nullable=false, length=32)
	private String benSalt1;
	
	@Column(nullable=false, length=32)
	private String benSalt2;
	
	@Column(nullable=false, length=64)
	private String benSalt3;
	
	@Column(nullable=false, length=64)
	private String benSalt4;
	
	@Column(nullable=false, length=128)
	private String benSalt5;
	
	@Column(nullable=false, length=128)
	private String benSalt6;
	
	@Column(nullable=false)
	private Timestamp startDate;
	
	@Column(nullable=false)
	private Timestamp expiryDate;
	
	@Column(nullable=false, length=50)
	private String createdBy;
	
	@Column(name="CreatedDate", insertable=false, updatable=false)
	private Timestamp createdDate;
}
