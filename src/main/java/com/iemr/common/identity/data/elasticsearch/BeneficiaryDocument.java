package com.iemr.common.identity.data.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BeneficiaryDocument {
    
    @JsonProperty("benId")
    private String benId;
    
    @JsonProperty("benRegId")
    private Long benRegId;
    
    @JsonProperty("phoneNum")
    private String phoneNum;
    
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
    
    @JsonProperty("gender")
    private String gender;
    
    @JsonProperty("districtName")
    private String districtName;
    
    @JsonProperty("villageName")
    private String villageName;
    
    @JsonProperty("aadharNo")
    private String aadharNo;
    
    @JsonProperty("govtIdentityNo")
    private String govtIdentityNo;
}