package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.repo.BenMappingRepo;

/**
 * Optimized service to fetch complete beneficiary data in bulk
 * Uses the new complete data query from BenMappingRepo
 */
@Service
public class OptimizedBeneficiaryDataService {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedBeneficiaryDataService.class);

    @Autowired
    private BenMappingRepo mappingRepo;

    /**
     * Fetch multiple beneficiaries with COMPLETE data in ONE query
     * This is the KEY method that replaces multiple individual queries
     */
    @Transactional(readOnly = true, timeout = 30)
    public List<BeneficiaryDocument> getBeneficiariesBatch(List<BigInteger> benRegIds) {
        if (benRegIds == null || benRegIds.isEmpty()) {
            return new ArrayList<>();
        }

        try {
            logger.debug("Fetching {} beneficiaries with complete data", benRegIds.size());
            
            // Use the new complete query from repo
            List<Object[]> results = mappingRepo.findCompleteDataByBenRegIds(benRegIds);
            
            logger.info("Fetched {} complete beneficiary records", results.size());

            List<BeneficiaryDocument> documents = new ArrayList<>();

            for (Object[] row : results) {
                try {
                    BeneficiaryDocument doc = mapRowToDocument(row);
                    if (doc != null && doc.getBenId() != null) {
                        documents.add(doc);
                    }
                } catch (Exception e) {
                    logger.error("Error mapping row to document: {}", e.getMessage(), e);
                }
            }

            logger.debug("Successfully converted {} beneficiaries to documents", documents.size());
            return documents;

        } catch (Exception e) {
            logger.error("Error fetching beneficiaries batch: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Single beneficiary fetch (for backward compatibility)
     */
    @Transactional(readOnly = true, timeout = 10)
    public BeneficiaryDocument getBeneficiaryFromDatabase(BigInteger benRegId) {
        List<BigInteger> ids = new ArrayList<>();
        ids.add(benRegId);
        
        List<BeneficiaryDocument> results = getBeneficiariesBatch(ids);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * Map database row to BeneficiaryDocument for ES
     * Matches the query column order from BenMappingRepo
     */
    private BeneficiaryDocument mapRowToDocument(Object[] row) {
        BeneficiaryDocument doc = new BeneficiaryDocument();
        
        try {
            int idx = 0;
            
            // Basic IDs (0-1)
            Long benRegId = getLong(row[idx++]);
            doc.setBenRegId(benRegId);
            String beneficiaryRegID = getString(row[idx++]);  // Column 1: d.BeneficiaryRegID

            // doc.setBenId(benRegId != null ? benRegId.toString() : null);
             if (beneficiaryRegID != null && !beneficiaryRegID.isEmpty()) {
                doc.setBenId(beneficiaryRegID);
            } else {
                doc.setBenId(benRegId != null ? benRegId.toString() : null);
            }
            doc.setBeneficiaryID(beneficiaryRegID);
            
            // Personal Info (2-10)
            doc.setFirstName(getString(row[idx++]));
            doc.setLastName(getString(row[idx++]));
            doc.setGenderID(getInteger(row[idx++]));
            doc.setGenderName(getString(row[idx++]));
            doc.setGender(doc.getGenderName()); // Use genderName for gender
            doc.setDOB(getDate(row[idx++]));
            doc.setAge(getInteger(row[idx++]));
            doc.setFatherName(getString(row[idx++]));
            doc.setSpouseName(getString(row[idx++]));
            doc.setIsHIVPos(getString(row[idx++]));
            
            // Metadata (11-14)
            doc.setCreatedBy(getString(row[idx++]));
            doc.setCreatedDate(getDate(row[idx++]));
            doc.setLastModDate(getLong(row[idx++]));
            doc.setBenAccountID(getLong(row[idx++]));
            
            // Contact (15)
            doc.setPhoneNum(getString(row[idx++]));
            
            // Health IDs (16-18)
            doc.setHealthID(getString(row[idx++]));
            doc.setAbhaID(getString(row[idx++]));
            doc.setFamilyID(getString(row[idx++]));
            
            // Current Address (19-30)
            doc.setStateID(getInteger(row[idx++]));
            doc.setStateName(getString(row[idx++]));
            doc.setDistrictID(getInteger(row[idx++]));
            doc.setDistrictName(getString(row[idx++]));
            doc.setBlockID(getInteger(row[idx++]));
            doc.setBlockName(getString(row[idx++]));
            doc.setVillageID(getInteger(row[idx++]));
            doc.setVillageName(getString(row[idx++]));
            doc.setPinCode(getString(row[idx++]));
            doc.setServicePointID(getInteger(row[idx++]));
            doc.setServicePointName(getString(row[idx++]));
            doc.setParkingPlaceID(getInteger(row[idx++]));
            
            // Permanent Address (31-38)
            doc.setPermStateID(getInteger(row[idx++]));
            doc.setPermStateName(getString(row[idx++]));
            doc.setPermDistrictID(getInteger(row[idx++]));
            doc.setPermDistrictName(getString(row[idx++]));
            doc.setPermBlockID(getInteger(row[idx++]));
            doc.setPermBlockName(getString(row[idx++]));
            doc.setPermVillageID(getInteger(row[idx++]));
            doc.setPermVillageName(getString(row[idx++]));
            
            // Identity (39-40)
            // doc.setGovtIdentityNo(getString(row[idx++]));
            // String aadhar = getString(row[idx]);
            // doc.setAadharNo(aadhar != null ? aadhar : doc.getGovtIdentityNo());
            
        } catch (Exception e) {
            logger.error("Error mapping row to document: {}", e.getMessage(), e);
        }
        
        return doc;
    }
    
    // Helper methods
    private String getString(Object value) {
        return value != null ? value.toString() : null;
    }
    
    private Long getLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Integer) return ((Integer) value).longValue();
        if (value instanceof BigInteger) return ((BigInteger) value).longValue();
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private Integer getInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Long) return ((Long) value).intValue();
        if (value instanceof BigInteger) return ((BigInteger) value).intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private java.util.Date getDate(Object value) {
        if (value == null) return null;
        if (value instanceof java.util.Date) return (java.util.Date) value;
        if (value instanceof java.sql.Timestamp) 
            return new java.util.Date(((java.sql.Timestamp) value).getTime());
        if (value instanceof java.sql.Date) 
            return new java.util.Date(((java.sql.Date) value).getTime());
        return null;
    }
}