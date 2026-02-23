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

package com.iemr.common.identity.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class BeneficiaryESMapper {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public Map<String, Object> transformESResponse(List<Map<String, Object>> esResults) {
        List<Map<String, Object>> transformedData = new ArrayList<>();
        
        for (Map<String, Object> esDoc : esResults) {
            Map<String, Object> beneficiary = new HashMap<>();
            
            beneficiary.put("beneficiaryRegID", esDoc.get("beneficiaryRegID"));
            beneficiary.put("beneficiaryID", esDoc.get("beneficiaryID"));
            beneficiary.put("firstName", esDoc.get("firstName"));
            beneficiary.put("lastName", esDoc.get("lastName"));
            beneficiary.put("genderID", esDoc.get("genderID"));
            beneficiary.put("genderName", esDoc.get("genderName"));
            beneficiary.put("dOB", esDoc.get("dOB"));
            beneficiary.put("dob", esDoc.get("dOB"));
            beneficiary.put("age", esDoc.get("age"));
            beneficiary.put("actualAge", esDoc.get("age"));
            beneficiary.put("ageUnits", "Years");
            beneficiary.put("fatherName", esDoc.getOrDefault("fatherName", ""));
            beneficiary.put("spouseName", esDoc.getOrDefault("spouseName", ""));
            beneficiary.put("isHIVPos", esDoc.getOrDefault("isHIVPos", ""));
            beneficiary.put("createdBy", esDoc.get("createdBy"));
            beneficiary.put("createdDate", esDoc.get("createdDate"));
            beneficiary.put("lastModDate", esDoc.get("lastModDate"));
            beneficiary.put("benAccountID", esDoc.get("benAccountID"));
            
            Map<String, Object> mGender = new HashMap<>();
            mGender.put("genderID", esDoc.get("genderID"));
            mGender.put("genderName", esDoc.get("genderName"));
            beneficiary.put("m_gender", mGender);
            
            Map<String, Object> demographics = (Map<String, Object>) esDoc.get("demographics");
            if (demographics != null) {
                Map<String, Object> benDemographics = new HashMap<>(demographics);
                benDemographics.put("beneficiaryRegID", esDoc.get("beneficiaryRegID"));
                
                benDemographics.put("m_state", createStateObject(demographics));
                benDemographics.put("m_district", createDistrictObject(demographics));
                benDemographics.put("m_districtblock", createBlockObject(demographics));
                benDemographics.put("m_districtbranchmapping", createBranchObject(demographics));
                
                beneficiary.put("i_bendemographics", benDemographics);
            }
            
            List<Map<String, Object>> phoneMaps = new ArrayList<>();
            List<Map<String, Object>> phoneNumbers = (List<Map<String, Object>>) esDoc.get("phoneNumbers");
            if (phoneNumbers != null && !phoneNumbers.isEmpty()) {
                for (Map<String, Object> phone : phoneNumbers) {
                    Map<String, Object> phoneMap = new HashMap<>(phone);
                    phoneMap.put("benificiaryRegID", esDoc.get("beneficiaryRegID"));
                    
                    Map<String, Object> relationType = new HashMap<>();
                    relationType.put("benRelationshipID", phone.get("benRelationshipID"));
                    relationType.put("benRelationshipType", phone.get("benRelationshipType"));
                    phoneMap.put("benRelationshipType", relationType);
                    
                    phoneMaps.add(phoneMap);
                }
            }
            beneficiary.put("benPhoneMaps", phoneMaps);
            
            beneficiary.put("isConsent", false);
            beneficiary.put("m_title", new HashMap<>());
            beneficiary.put("maritalStatus", new HashMap<>());
            beneficiary.put("changeInSelfDetails", false);
            beneficiary.put("changeInAddress", false);
            beneficiary.put("changeInContacts", false);
            beneficiary.put("changeInIdentities", false);
            beneficiary.put("changeInOtherDetails", false);
            beneficiary.put("changeInFamilyDetails", false);
            beneficiary.put("changeInAssociations", false);
            beneficiary.put("changeInBankDetails", false);
            beneficiary.put("changeInBenImage", false);
            beneficiary.put("is1097", false);
            beneficiary.put("emergencyRegistration", false);
            beneficiary.put("passToNurse", false);
            beneficiary.put("beneficiaryIdentities", new ArrayList<>());
            
            transformedData.add(beneficiary);
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("data", transformedData);
        response.put("statusCode", 200);
        response.put("errorMessage", "Success");
        response.put("status", "Success");
        
        return response;
    }
    
    private Map<String, Object> createStateObject(Map<String, Object> demographics) {
        Map<String, Object> state = new HashMap<>();
        state.put("stateID", demographics.get("stateID"));
        state.put("stateName", demographics.get("stateName"));
        state.put("stateCode", demographics.get("stateCode"));
        state.put("countryID", 1);
        return state;
    }
    
    private Map<String, Object> createDistrictObject(Map<String, Object> demographics) {
        Map<String, Object> district = new HashMap<>();
        district.put("districtID", demographics.get("districtID"));
        district.put("districtName", demographics.get("districtName"));
        district.put("stateID", demographics.get("stateID"));
        return district;
    }
    
    private Map<String, Object> createBlockObject(Map<String, Object> demographics) {
        Map<String, Object> block = new HashMap<>();
        block.put("blockID", demographics.get("blockID"));
        block.put("blockName", demographics.get("blockName"));
        block.put("districtID", demographics.get("districtID"));
        block.put("stateID", demographics.get("stateID"));
        return block;
    }
    
    private Map<String, Object> createBranchObject(Map<String, Object> demographics) {
        Map<String, Object> branch = new HashMap<>();
        branch.put("districtBranchID", demographics.get("districtBranchID"));
        branch.put("blockID", demographics.get("blockID"));
        branch.put("villageName", demographics.get("districtBranchName"));
        branch.put("pinCode", demographics.get("pinCode"));
        return branch;
    }
}