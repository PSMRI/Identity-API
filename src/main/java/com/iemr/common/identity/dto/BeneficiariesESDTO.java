package com.iemr.common.identity.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BeneficiariesESDTO {
    
    @JsonProperty("benRegId")
    private Long benRegId;
    
    @JsonProperty("beneficiaryID")
    private String beneficiaryID;
    
    @JsonProperty("firstName")
    private String firstName;
    
    @JsonProperty("lastName")
    private String lastName;
    
    @JsonProperty("genderID")
    private Integer genderID;
    
    @JsonProperty("genderName")
    private String genderName;
    
    @JsonProperty("dOB")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Date dOB;
    
    @JsonProperty("age")
    private Integer age;
    
    @JsonProperty("phoneNum")
    private String phoneNum;
    
    // @JsonProperty("aadharNo")
    // private String aadharNo;
    
    // @JsonProperty("govtIdentityNo")
    // private String govtIdentityNo;
    
    @JsonProperty("fatherName")
    private String fatherName;
    
    @JsonProperty("spouseName")
    private String spouseName;
    
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
    
    @JsonProperty("healthID")
    private String healthID;
    
    @JsonProperty("abhaID")
    private String abhaID;
    
    @JsonProperty("familyID")
    private String familyID;
    
    @JsonProperty("stateID")
    private Integer stateID;
    
    @JsonProperty("stateName")
    private String stateName;
    
    @JsonProperty("stateCode")
    private String stateCode;
    
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
    
    @JsonProperty("districtBranchID")
    private Integer districtBranchID;
    
    @JsonProperty("districtBranchName")
    private String districtBranchName;
    
    @JsonProperty("parkingPlaceID")
    private Integer parkingPlaceID;
    
    @JsonProperty("servicePointID")
    private Integer servicePointID;
    
    @JsonProperty("servicePointName")
    private String servicePointName;
    
    @JsonProperty("pinCode")
    private String pinCode;
    
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
    
    @JsonProperty("phoneNumbers")
    private List<PhoneNumberDTO> phoneNumbers;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PhoneNumberDTO {
        @JsonProperty("benPhMapID")
        private Long benPhMapID;
        
        @JsonProperty("phoneNo")
        private String phoneNo;
        
        @JsonProperty("parentBenRegID")
        private Long parentBenRegID;
        
        @JsonProperty("benRelationshipID")
        private Integer benRelationshipID;
        
        @JsonProperty("benRelationshipType")
        private String benRelationshipType;
    }
}