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
    
    @JsonProperty("lastName")
    private String lastName;
    
    @JsonProperty("fatherName")
    private String fatherName;
    
    @JsonProperty("spouseName")
    private String spouseName;
    
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