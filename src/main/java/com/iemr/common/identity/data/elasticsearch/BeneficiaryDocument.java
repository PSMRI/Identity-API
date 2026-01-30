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

package com.iemr.common.identity.data.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.Date;

@Data
public class BeneficiaryDocument {
    
    @JsonProperty("benId")
    private String benId;
    
    @JsonProperty("benRegId")
    private Long benRegId;
    
    @JsonProperty("beneficiaryID")
    private String beneficiaryID;
    
    @JsonProperty("firstName")
    private String firstName;
    
    @JsonProperty("middleName")
    private String middleName;

    @JsonProperty("lastName")
    private String lastName;
    
    @JsonProperty("fatherName")
    private String fatherName;
    
    @JsonProperty("spouseName")
    private String spouseName;
    
    @JsonProperty("maritalStatusID")
    private Integer maritalStatusID;    

    @JsonProperty("maritalStatusName")
    private String maritalStatusName;
    
    @JsonProperty("age")
    private Integer age;
    
    @JsonProperty("dOB")
    private Date dOB;
    
    @JsonProperty("gender")
    private String gender;
    
    @JsonProperty("genderID")
    private Integer genderID;
    
    @JsonProperty("genderName")
    private String genderName;
    
    @JsonProperty("phoneNum")
    private String phoneNum;
    
    @JsonProperty("aadharNo")
    private String aadharNo;
    
    @JsonProperty("govtIdentityNo")
    private String govtIdentityNo;
    
    @JsonProperty("healthID")
    private String healthID;
    
    @JsonProperty("abhaID")
    private String abhaID;
    
    @JsonProperty("abhaCreatedDate")
    private String abhaCreatedDate;

    @JsonProperty("familyID")
    private String familyID;
    
    @JsonProperty("stateID")
    private Integer stateID;
    
    @JsonProperty("stateName")
    private String stateName;
    
    @JsonProperty("districtID")
    private Integer districtID;
    
    @JsonProperty("districtName")
    private String districtName;
    
    @JsonProperty("blockID")
    private Integer blockID;
    
    @JsonProperty("blockName")
    private String blockName;
    
    @JsonProperty("villageID")
    private Integer villageID;
    
    @JsonProperty("villageName")
    private String villageName;
    
    @JsonProperty("pinCode")
    private String pinCode;
    
    @JsonProperty("servicePointID")
    private Integer servicePointID;
    
    @JsonProperty("servicePointName")
    private String servicePointName;
    
    @JsonProperty("parkingPlaceID")
    private Integer parkingPlaceID;
    
    @JsonProperty("permStateID")
    private Integer permStateID;
    
    @JsonProperty("permStateName")
    private String permStateName;
    
    @JsonProperty("permDistrictID")
    private Integer permDistrictID;
    
    @JsonProperty("permDistrictName")
    private String permDistrictName;
    
    @JsonProperty("permBlockID")
    private Integer permBlockID;
    
    @JsonProperty("permBlockName")
    private String permBlockName;
    
    @JsonProperty("permVillageID")
    private Integer permVillageID;
    
    @JsonProperty("permVillageName")
    private String permVillageName;
    
    @JsonProperty("createdBy")
    private String createdBy;
    
    @JsonProperty("createdDate")
    private Date createdDate;
    
    @JsonProperty("lastModDate")
    private Long lastModDate;
    
    @JsonProperty("benAccountID")
    private Long benAccountID;
    
    @JsonProperty("isHIVPos")
    private String isHIVPos;
}