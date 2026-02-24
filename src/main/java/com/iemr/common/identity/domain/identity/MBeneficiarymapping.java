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
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;

import com.google.gson.annotations.Expose;

import lombok.Data;

/**
 * The persistent class for the m_beneficiarymapping database table.
 * 
 */
@Entity
@Table(name = "i_beneficiarymapping")
@NamedQuery(name = "MBeneficiarymapping.findAll", query = "SELECT m FROM MBeneficiarymapping m")
@Data
public class MBeneficiarymapping implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(unique = true, nullable = false)
	private BigInteger benMapId;

	@Column(nullable = false, length = 50)
	private String createdBy;

	@Column(name = "CreatedDate",updatable = false)
	private Timestamp createdDate;

	@Column(nullable = false)
	private Boolean deleted = false;

	private Timestamp lastModDate;

	@Column(length = 50)
	private String modifiedBy;

	@Column(nullable = false, length = 4, insertable = false)
	private String processed = "N";

	private Boolean reserved;

	private Integer reservedById;

	@Column(length = 45)
	private String reservedFor;

	private Timestamp reservedOn;

	// uni-directional one-to-one association to MBeneficiaryaddress
	@OneToOne
	@JoinColumn(name = "BenAddressId", nullable = false)
	private MBeneficiaryaddress mBeneficiaryaddress;

	@Column(name = "BenAddressId", insertable = false, updatable = false)
	private BigInteger benAddressId;

	// uni-directional one-to-one association to MBeneficiaryconsent
	@OneToOne
	@JoinColumn(name = "BenConsentId"/* , nullable=false */)
	private MBeneficiaryconsent mBeneficiaryconsent;

	@Column(name = "BenConsentId", insertable = false, updatable = false)
	private BigInteger benConsentId;

	// uni-directional one-to-one association to MBeneficiarycontact
	@OneToOne
	@JoinColumn(name = "BenContactsId", nullable = false)
	private MBeneficiarycontact mBeneficiarycontact;

	@Column(name = "BenContactsId", insertable = false, updatable = false)
	private BigInteger benContactsId;

	// uni-directional one-to-one association to MBeneficiarydetail
	@OneToOne
	@JoinColumn(name = "BenDetailsId", referencedColumnName = "BeneficiaryDetailsId", nullable = false)
	private MBeneficiarydetail mBeneficiarydetail;

	@Column(name = "BenDetailsId", insertable = false, updatable = false)
	private BigInteger benDetailsId;

	// uni-directional one-to-one association to MBeneficiaryregidmapping
	@OneToOne
	@JoinColumn(name = "BenRegId"/* , nullable=false */)
	private MBeneficiaryregidmapping mBeneficiaryregidmapping;

	// adding BenRegId variable to get BenMapId for edit beneficiary details
	@Column(name = "BenRegId", insertable = false, updatable = false)
	private BigInteger benRegId;

	// uni-directional one-to-one association to MBensecurestack
	@OneToOne
	@JoinColumn(name = "BenSecureStackId", nullable = false)
	private MBensecurestack mBeneficiarysecurestack;

	// uni-directional, new, 28-09-2018
	// bi-directional many-to-one association to MBeneficiaryfamilymapping
	// @OneToMany(mappedBy="MBeneficiarymapping", fetch=FetchType.EAGER)
	// @Fetch(FetchMode.SUBSELECT)
	@OneToMany
	@JoinColumn(referencedColumnName = "benMapId", name = "benMapId", insertable = false, updatable = false)
	private List<MBeneficiaryfamilymapping> mBeneficiaryfamilymappings;

	// uni-directional, new, 28-09-2018
	// bi-directional many-to-one association to MBeneficiaryidentity
	// @OneToMany(mappedBy = "MBeneficiarymapping", fetch = FetchType.EAGER)
	// @Fetch(FetchMode.SUBSELECT)
	@OneToMany
	@JoinColumn(referencedColumnName = "benMapId", name = "benMapId", insertable = false, updatable = false)
	private List<MBeneficiaryidentity> mBeneficiaryidentities;

	// uni-directional, new, 28-09-2018
	// bi-directional many-to-one association to MBeneficiaryservicemapping
	// @OneToMany(mappedBy = "MBeneficiarymapping", fetch = FetchType.EAGER)
	// @Fetch(FetchMode.SUBSELECT)
	@OneToMany
	@JoinColumn(referencedColumnName = "benMapId", name = "benMapId", insertable = false, updatable = false)
	private List<MBeneficiaryservicemapping> mBeneficiaryservicemappings;

	// uni-directional, new, 28-09-2018
	// bi-directional many-to-one association to TBendataaccess
	// @OneToMany(mappedBy = "MBeneficiarymapping", fetch = FetchType.EAGER)
	// @Fetch(FetchMode.SUBSELECT)
	@OneToMany
	@JoinColumn(referencedColumnName = "benMapId", name = "benMapId", insertable = false, updatable = false)
	private List<TBendataaccess> tBendataaccesses;

	/*
	 * New columns added for MMU integration 11-04-2018
	 */
	// uni-directional one-to-one association
	@OneToOne
	@JoinColumn(name = "BenImageId")
	private MBeneficiaryImage mBeneficiaryImage;

	@Column(name = "benImageId", insertable = false, updatable = false)
	private BigInteger benImageId;

	// uni-directional one-to-one association
	@OneToOne
	@JoinColumn(name = "BenAccountID")
	private MBeneficiaryAccount mBeneficiaryAccount;

	@Column(name = "benAccountID", insertable = false, updatable = false)
	private BigInteger benAccountID;

	// End

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

	// 20-01-2021, added for D2D
	@Expose
	@Transient
	private Long houseHoldID;

	@Expose
	@Transient
	private String guideLineID;

	@Expose
	@Transient
	private String rchID;

	public MBeneficiarymapping() {
	}

	public MBeneficiarymapping(BigInteger benMapId, BigInteger benAddressId, BigInteger benConsentId,
			BigInteger benContactsId, BigInteger benDetailsId, BigInteger benRegId, BigInteger benImageId, BigInteger benAccountID,
			Integer vanID, BigInteger vanSerialNo, String createdBy, Timestamp createdDate) {
		super();
		this.benMapId = benMapId;
		this.benAddressId = benAddressId;
		this.benConsentId = benConsentId;
		this.benContactsId = benContactsId;
		this.benDetailsId = benDetailsId;
		this.benRegId = benRegId;
		this.benImageId = benImageId;
		this.benAccountID = benAccountID;
		this.vanID = vanID;
		this.vanSerialNo = vanSerialNo;
		this.createdBy = createdBy;
		this.createdDate = createdDate;
	}

	// END OF new column added for data sync

}