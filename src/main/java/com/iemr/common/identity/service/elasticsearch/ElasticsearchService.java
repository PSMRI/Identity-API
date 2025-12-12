package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.iemr.common.identity.dto.BeneficiariesESDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import com.iemr.common.identity.repo.BenDetailRepo;

@Service
public class ElasticsearchService {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);
    
    @Autowired
    private ElasticsearchClient esClient;
    
    @Autowired
    private BenDetailRepo benRegRepository;
    
    @Value("${elasticsearch.index.beneficiary:beneficiary_index_v4}")
    private String beneficiaryIndex;
    
   public List<Map<String, Object>> universalSearch(String query) {
    logger.info("Universal ES search for: {}", query);
    
    try {
        boolean isNumeric = query.matches("\\d+");
        
        SearchResponse<BeneficiariesESDTO> response = esClient.search(s -> s
            .index(beneficiaryIndex)
            .query(q -> q
                .bool(b -> {
                    // Fuzzy multi-match for text fields
                    b.should(s1 -> s1.multiMatch(mm -> mm
                        .query(query)
                        .fields("firstName^3", "lastName^2", "fatherName", "spouseName")
                        .type(TextQueryType.BestFields)
                        .fuzziness("AUTO")
                    ));
                    
                    // Exact match for phone number
                    b.should(s2 -> s2.term(t -> t
                        .field("phoneNum")
                        .value(query)
                    ));
                    
                    // Numeric fields (only if query is numeric)
                    if (isNumeric) {
                        try {
                            Long numericValue = Long.parseLong(query);
                            b.should(s3 -> s3.term(t -> t.field("benRegId").value(numericValue)));
                        } catch (NumberFormatException e) {
                            logger.debug("Could not parse as long: {}", query);
                        }
                        
                        b.should(s4 -> s4.term(t -> t.field("benId").value(query)));
                    }
                    
                    b.minimumShouldMatch("1");
                    return b;
                })
            )
            .size(20)
        , BeneficiariesESDTO.class);
        
        logger.info("ES returned {} hits", response.hits().hits().size());
        
        // Convert ES results directly to response format
        List<Map<String, Object>> results = response.hits().hits().stream()
            .map(hit -> mapESResultToExpectedFormat(hit.source()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        
        if (results.isEmpty()) {
            logger.info("No results found in ES, falling back to database");
            return searchInDatabaseDirectly(query);
        }
        
        logger.info("Returning {} results from ES", results.size());
        return results;
        
    } catch (Exception e) {
        logger.error("ES universal search failed: {}", e.getMessage(), e);
        logger.info("Fallback: Searching in MySQL database");
        return searchInDatabaseDirectly(query);
    }
}

/**
 * Map ES DTO directly to expected API format (NO DATABASE CALL)
 */
private Map<String, Object> mapESResultToExpectedFormat(BeneficiariesESDTO esData) {
    if (esData == null) {
        return null;
    }
    
    Map<String, Object> result = new HashMap<>();
    
    try {
        // Basic fields from ES
        result.put("beneficiaryRegID", esData.getBenRegId());
        result.put("beneficiaryID", esData.getBeneficiaryID());
        result.put("firstName", esData.getFirstName());
        result.put("lastName", esData.getLastName());
        result.put("genderID", esData.getGenderID());
        result.put("genderName", esData.getGenderName());
        // result.put("dOB", esData.getdOB());
        result.put("dob", esData.getDOB());
        result.put("age", esData.getAge());
        result.put("actualAge", esData.getAge());
        result.put("ageUnits", "Years");
        result.put("fatherName", esData.getFatherName() != null ? esData.getFatherName() : "");
        result.put("spouseName", esData.getSpouseName() != null ? esData.getSpouseName() : "");
        result.put("isHIVPos", "");
        result.put("createdBy", esData.getCreatedBy());
        result.put("createdDate", esData.getCreatedDate());
        result.put("lastModDate", esData.getLastModDate());
        result.put("benAccountID", esData.getBenAccountID());
        
        // Gender object
        Map<String, Object> mGender = new HashMap<>();
        mGender.put("genderID", esData.getGenderID());
        mGender.put("genderName", esData.getGenderName());
        result.put("m_gender", mGender);
        
        // Demographics object from ES
        Map<String, Object> demographics = new HashMap<>();
        demographics.put("beneficiaryRegID", esData.getBenRegId());
        demographics.put("stateID", esData.getStateID());
        demographics.put("stateName", esData.getStateName());
        demographics.put("districtID", esData.getDistrictID());
        demographics.put("districtName", esData.getDistrictName());
        demographics.put("blockID", esData.getBlockID());
        demographics.put("blockName", esData.getBlockName());
        demographics.put("districtBranchID", null);
        demographics.put("districtBranchName", null);
        demographics.put("parkingPlaceID", esData.getParkingPlaceID());
        demographics.put("servicePointID", esData.getServicePointID());
        demographics.put("servicePointName", esData.getServicePointName());
        demographics.put("createdBy", esData.getCreatedBy());
        
        // Nested m_state
        Map<String, Object> mState = new HashMap<>();
        mState.put("stateID", esData.getStateID());
        mState.put("stateName", esData.getStateName());
        mState.put("stateCode", null);
        mState.put("countryID", 1);
        demographics.put("m_state", mState);
        
        // Nested m_district
        Map<String, Object> mDistrict = new HashMap<>();
        mDistrict.put("districtID", esData.getDistrictID());
        mDistrict.put("districtName", esData.getDistrictName());
        mDistrict.put("stateID", esData.getStateID());
        demographics.put("m_district", mDistrict);
        
        // Nested m_districtblock
        Map<String, Object> mBlock = new HashMap<>();
        mBlock.put("blockID", esData.getBlockID());
        mBlock.put("blockName", esData.getBlockName());
        mBlock.put("districtID", esData.getDistrictID());
        mBlock.put("stateID", esData.getStateID());
        demographics.put("m_districtblock", mBlock);
        
        // Nested m_districtbranchmapping
        Map<String, Object> mBranch = new HashMap<>();
        mBranch.put("districtBranchID", null);
        mBranch.put("blockID", esData.getBlockID());
        mBranch.put("villageName", null);
        mBranch.put("pinCode", esData.getPinCode());
        demographics.put("m_districtbranchmapping", mBranch);
        
        result.put("i_bendemographics", demographics);
        
        // Phone numbers from ES
        List<Map<String, Object>> benPhoneMaps = new ArrayList<>();
        if (esData.getPhoneNum() != null && !esData.getPhoneNum().isEmpty()) {
            Map<String, Object> phoneMap = new HashMap<>();
            phoneMap.put("benPhMapID", 1L);
            phoneMap.put("benificiaryRegID", esData.getBenRegId());
            phoneMap.put("parentBenRegID", esData.getBenRegId());
            phoneMap.put("benRelationshipID", 1);
            phoneMap.put("phoneNo", esData.getPhoneNum());
            
            Map<String, Object> relationType = new HashMap<>();
            relationType.put("benRelationshipID", 1);
            relationType.put("benRelationshipType", "Self");
            phoneMap.put("benRelationshipType", relationType);
            
            benPhoneMaps.add(phoneMap);
        }
        result.put("benPhoneMaps", benPhoneMaps);
        
        // Default values
        result.put("isConsent", false);
        result.put("m_title", new HashMap<>());
        result.put("maritalStatus", new HashMap<>());
        result.put("changeInSelfDetails", false);
        result.put("changeInAddress", false);
        result.put("changeInContacts", false);
        result.put("changeInIdentities", false);
        result.put("changeInOtherDetails", false);
        result.put("changeInFamilyDetails", false);
        result.put("changeInAssociations", false);
        result.put("changeInBankDetails", false);
        result.put("changeInBenImage", false);
        result.put("is1097", false);
        result.put("emergencyRegistration", false);
        result.put("passToNurse", false);
        result.put("beneficiaryIdentities", new ArrayList<>());
        
    } catch (Exception e) {
        logger.error("Error mapping ES result: {}", e.getMessage(), e);
        return null;
    }
    
    return result;
}
    
    /**
     * Fetch complete beneficiary data from database using IDs from ES
     */
    private List<Map<String, Object>> fetchCompleteDataFromDatabase(List<Long> beneficiaryIds) {
        try {
            List<Object[]> results = benRegRepository.findCompleteDataByIds(beneficiaryIds);
            
            return results.stream()
                .map(this::mapToExpectedFormat)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Failed to fetch complete data from database: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Direct database search as fallback
     */
    private List<Map<String, Object>> searchInDatabaseDirectly(String query) {
        try {
            List<Object[]> results = benRegRepository.searchBeneficiaries(query);
            
            return results.stream()
                .map(this::mapToExpectedFormat)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Database search failed: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Map database result to expected API format
     */
    private Map<String, Object> mapToExpectedFormat(Object[] row) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Basic fields
            Long beneficiaryRegID = getLong(row[0]);
            String beneficiaryID = getString(row[1]);
            String firstName = getString(row[2]);
            String lastName = getString(row[3]);
            Integer genderID = getInteger(row[4]);
            String genderName = getString(row[5]);
            Date dob = getDate(row[6]);
            Integer age = getInteger(row[7]);
            String fatherName = getString(row[8]);
            String spouseName = getString(row[9]);
            String isHIVPos = getString(row[10]);
            String createdBy = getString(row[11]);
            Date createdDate = getDate(row[12]);
            Long lastModDate = getLong(row[13]);
            Long benAccountID = getLong(row[14]);
            
            // Demographics
            Integer stateID = getInteger(row[15]);
            String stateName = getString(row[16]);
            Integer districtID = getInteger(row[17]);
            String districtName = getString(row[18]);
            Integer blockID = getInteger(row[19]);
            String blockName = getString(row[20]);
            String pinCode = getString(row[21]);
            Integer servicePointID = getInteger(row[22]);
            String servicePointName = getString(row[23]);
            Integer parkingPlaceID = getInteger(row[24]);
            String phoneNum = getString(row[25]);
            
            // Build result
            result.put("beneficiaryRegID", beneficiaryRegID);
            result.put("beneficiaryID", beneficiaryID);
            result.put("firstName", firstName);
            result.put("lastName", lastName);
            result.put("genderID", genderID);
            result.put("genderName", genderName);
            result.put("dOB", dob);
            result.put("dob", dob);
            result.put("age", age);
            result.put("actualAge", age);
            result.put("ageUnits", "Years");
            result.put("fatherName", fatherName != null ? fatherName : "");
            result.put("spouseName", spouseName != null ? spouseName : "");
            result.put("isHIVPos", isHIVPos != null ? isHIVPos : "");
            result.put("createdBy", createdBy);
            result.put("createdDate", createdDate);
            result.put("lastModDate", lastModDate);
            result.put("benAccountID", benAccountID);
            
            // Gender object
            Map<String, Object> mGender = new HashMap<>();
            mGender.put("genderID", genderID);
            mGender.put("genderName", genderName);
            result.put("m_gender", mGender);
            
            // Demographics object
            Map<String, Object> demographics = new HashMap<>();
            demographics.put("beneficiaryRegID", beneficiaryRegID);
            demographics.put("stateID", stateID);
            demographics.put("stateName", stateName);
            demographics.put("districtID", districtID);
            demographics.put("districtName", districtName);
            demographics.put("blockID", blockID);
            demographics.put("blockName", blockName);
            demographics.put("districtBranchID", null); // Not in your tables
            demographics.put("districtBranchName", null);
            demographics.put("parkingPlaceID", parkingPlaceID);
            demographics.put("servicePointID", servicePointID);
            demographics.put("servicePointName", servicePointName);
            demographics.put("createdBy", createdBy);
            
            // Nested m_state
            Map<String, Object> mState = new HashMap<>();
            mState.put("stateID", stateID);
            mState.put("stateName", stateName);
            mState.put("stateCode", null); // Not available in your tables
            mState.put("countryID", 1);
            demographics.put("m_state", mState);
            
            // Nested m_district
            Map<String, Object> mDistrict = new HashMap<>();
            mDistrict.put("districtID", districtID);
            mDistrict.put("districtName", districtName);
            mDistrict.put("stateID", stateID);
            demographics.put("m_district", mDistrict);
            
            // Nested m_districtblock
            Map<String, Object> mBlock = new HashMap<>();
            mBlock.put("blockID", blockID);
            mBlock.put("blockName", blockName);
            mBlock.put("districtID", districtID);
            mBlock.put("stateID", stateID);
            demographics.put("m_districtblock", mBlock);
            
            // Nested m_districtbranchmapping
            Map<String, Object> mBranch = new HashMap<>();
            mBranch.put("districtBranchID", null);
            mBranch.put("blockID", blockID);
            mBranch.put("villageName", null);
            mBranch.put("pinCode", pinCode);
            demographics.put("m_districtbranchmapping", mBranch);
            
            result.put("i_bendemographics", demographics);
            
            // Phone numbers
            List<Map<String, Object>> benPhoneMaps = fetchPhoneNumbers(beneficiaryRegID);
            result.put("benPhoneMaps", benPhoneMaps);
            
            // Default values
            result.put("isConsent", false);
            result.put("m_title", new HashMap<>());
            result.put("maritalStatus", new HashMap<>());
            result.put("changeInSelfDetails", false);
            result.put("changeInAddress", false);
            result.put("changeInContacts", false);
            result.put("changeInIdentities", false);
            result.put("changeInOtherDetails", false);
            result.put("changeInFamilyDetails", false);
            result.put("changeInAssociations", false);
            result.put("changeInBankDetails", false);
            result.put("changeInBenImage", false);
            result.put("is1097", false);
            result.put("emergencyRegistration", false);
            result.put("passToNurse", false);
            result.put("beneficiaryIdentities", new ArrayList<>());
            
        } catch (Exception e) {
            logger.error("Error mapping result: {}", e.getMessage(), e);
        }
        
        return result;
    }
    
    /**
     * Fetch phone numbers for a beneficiary
     */
    private List<Map<String, Object>> fetchPhoneNumbers(Long beneficiaryRegID) {
        List<Map<String, Object>> phoneList = new ArrayList<>();
        
        try {
            List<Object[]> phones = benRegRepository.findPhoneNumbersByBeneficiaryId(beneficiaryRegID);
            
            int mapId = 1;
            for (Object[] phone : phones) {
                String phoneNo = getString(phone[0]);
                String phoneType = getString(phone[1]);
                
                if (phoneNo != null && !phoneNo.isEmpty()) {
                    Map<String, Object> phoneMap = new HashMap<>();
                    phoneMap.put("benPhMapID", (long) mapId++);
                    phoneMap.put("benificiaryRegID", beneficiaryRegID);
                    phoneMap.put("parentBenRegID", beneficiaryRegID);
                    phoneMap.put("benRelationshipID", 1);
                    phoneMap.put("phoneNo", phoneNo);
                    
                    Map<String, Object> relationType = new HashMap<>();
                    relationType.put("benRelationshipID", 1);
                    relationType.put("benRelationshipType", phoneType != null ? phoneType : "Self");
                    phoneMap.put("benRelationshipType", relationType);
                    
                    phoneList.add(phoneMap);
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching phone numbers: {}", e.getMessage(), e);
        }
        
        return phoneList;
    }
    
    // Helper methods
    private String getString(Object value) {
        if (value == null) return null;
        return value.toString();
    }
    
    private Long getLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Integer) return ((Integer) value).longValue();
        if (value instanceof BigDecimal) return ((BigDecimal) value).longValue();
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    private Integer getInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Long) return ((Long) value).intValue();
        if (value instanceof BigDecimal) return ((BigDecimal) value).intValue();
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    private Date getDate(Object value) {
        if (value == null) return null;
        if (value instanceof Date) return (Date) value;
        if (value instanceof Timestamp) return new Date(((Timestamp) value).getTime());
        if (value instanceof java.sql.Date) return new Date(((java.sql.Date) value).getTime());
        return null;
    }
}