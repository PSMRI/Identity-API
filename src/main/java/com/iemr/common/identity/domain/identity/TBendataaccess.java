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
import jakarta.persistence.Transient;

import lombok.Data;

/**
 * The persistent class for the t_bendataaccess database table.
 * 
 */
@Entity
@Table(name = "t_bendataaccess")
@NamedQuery(name = "TBendataaccess.findAll", query = "SELECT t FROM TBendataaccess t")
@Data
public class TBendataaccess implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger accessId;

	@Column(nullable = false, length = 45)
	private String accessedBy;

	@Column(nullable = false)
	private Timestamp accessedOn;

	@Column(nullable = false, length = 45)
	private String accessedType;

	@Column(nullable = false, length = 150)
	private String authId;

	@Transient
	private String consentId;

	@Column(length = 4)
	private String processed = "N";

	@Column(nullable = false)
	private Integer userAgentId;

	@Transient
	private String userAgentIpAddr;

	@Column(nullable = false)
	private Integer userAgentPSMapId;

	@Column(nullable = false)
	private Integer userAgentRoleId;

	@Column(nullable = false)
	private Integer userAgentServiceId;

	@Column(name = "BenMapId", insertable = true, updatable = true)
	private BigInteger benMapId;

}