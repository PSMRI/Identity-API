package com.iemr.common.identity.data.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;

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
    
    @JsonProperty("age")
    private Integer age;
    
    @JsonProperty("gender")
    private String gender;
    
    @JsonProperty("districtName")
    private String districtName;
    
    @JsonProperty("villageName")
    private String villageName;
    
    // Add other fields from BeneficiariesDTO that you need to search
    
    // Constructors
    public BeneficiaryDocument() {}
    
    // Getters and Setters
    public String getBenId() { return benId; }
    public void setBenId(String benId) { this.benId = benId; }
    
    public Long getBenRegId() { return benRegId; }
    public void setBenRegId(Long benRegId) { this.benRegId = benRegId; }
    
    public String getPhoneNum() { return phoneNum; }
    public void setPhoneNum(String phoneNum) { this.phoneNum = phoneNum; }
    
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }

    public String getGender() { return gender; }
    public void setGender(String gender) { this.gender = gender; }

    public String getDistrictName() { return districtName; }
    public void setDistrictName(String districtName) { this.districtName = districtName; }

    public String getVillageName() { return villageName; }
    public void setVillageName(String villageName) { this.villageName = villageName; }

}