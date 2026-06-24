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
package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.V_BenAdvanceSearchRepo;

/**
 * Optimized service to fetch complete beneficiary data in bulk
 * Uses the new complete data query from BenMappingRepo
 */
@Service
public class BeneficiaryDocumentDataService {

    private static final Logger logger = LoggerFactory.getLogger(BeneficiaryDocumentDataService.class);

    @Autowired
    private BenMappingRepo mappingRepo;

    @Autowired
    private V_BenAdvanceSearchRepo v_BenAdvanceSearchRepo;

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

            List<Object[]> results = mappingRepo.findCompleteDataByBenRegIds(benRegIds);

            logger.info("Fetched {} complete beneficiary records", results.size());
            // Batch fetch ABHA details for ALL beneficiaries at once
            Map<Long, AbhaData> abhaMap = batchFetchAbhaData(benRegIds);

            logger.info("Fetched ABHA details for {} beneficiaries", abhaMap.size());

            List<BeneficiaryDocument> documents = new ArrayList<>();

            for (Object[] row : results) {
                try {
                    BeneficiaryDocument doc = mapRowToDocument(row);
                    if (doc != null && doc.getBenId() != null) {

                        AbhaData abhaData = abhaMap.get(doc.getBenRegId());
                        if (abhaData != null) {
                            doc.setHealthID(abhaData.getHealthID());
                            doc.setAbhaID(abhaData.getHealthIDNumber());
                            doc.setAbhaCreatedDate(abhaData.getAbhaCreatedDate());
                            logger.info("Enriched benRegId={} with healthID={}, abhaID={}",
                                    doc.getBenRegId(), doc.getHealthID(), doc.getAbhaID());
                        } else {
                            logger.debug("No ABHA details for benRegId={}", doc.getBenRegId());
                        }
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

    private Map<Long, AbhaData> batchFetchAbhaData(List<BigInteger> benRegIds) {
        try {
            return batchFetchAbhaDetails(benRegIds);
        } catch (Exception e) {
            logger.warn("Error fetching ABHA details (will continue without ABHA data): {}", e.getMessage());
            return new HashMap<>(); // Return empty map to continue processing
        }
    }

    /**
     * Batch fetch ABHA details for multiple beneficiaries
     * Returns a map of benRegId -> AbhaData
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, timeout = 30)
    private Map<Long, AbhaData> batchFetchAbhaDetails(List<BigInteger> benRegIds) {
        Map<Long, AbhaData> abhaMap = new HashMap<>();

        try {
            if (benRegIds == null || benRegIds.isEmpty()) {
                logger.info("No beneficiary IDs provided for ABHA fetch");
                return abhaMap;
            }
            logger.debug("Batch fetching ABHA details for {} beneficiaries", benRegIds.size());

            List<Object[]> abhaRecords = null;
            try {
                abhaRecords = v_BenAdvanceSearchRepo.getBenAbhaDetailsByBenRegIDs(benRegIds);
            } catch (Exception e) {
                logger.warn("ABHA query returned error (likely no records): {}", e.getMessage());
                return abhaMap; // Return empty map - this is OK
            }

            if (abhaRecords == null || abhaRecords.isEmpty()) {
                logger.debug("No ABHA records found for this batch (this is normal)");
                return abhaMap;
            }

            logger.debug("Retrieved {} ABHA records", abhaRecords.size());

            for (Object[] record : abhaRecords) {
                try {
                    // record[0] -> BeneficiaryRegID
                    // record[1] -> HealthID (ABHA Address)
                    // record[2] -> HealthIDNumber (ABHA Number)
                    // record[3] -> AuthenticationMode
                    // record[4] -> CreatedDate

                    Long benRegId = null;
                    if (record[0] instanceof BigInteger) {
                        benRegId = ((BigInteger) record[0]).longValue();
                    } else if (record[0] instanceof Long) {
                        benRegId = (Long) record[0];
                    } else if (record[0] instanceof Integer) {
                        benRegId = ((Integer) record[0]).longValue();
                    }

                    if (benRegId != null && !abhaMap.containsKey(benRegId)) {
                        // Only store the first (most recent) record for each beneficiary
                        AbhaData abhaData = new AbhaData();
                        abhaData.setHealthID(record[1] != null ? record[1].toString() : null);
                        abhaData.setHealthIDNumber(record[2] != null ? record[2].toString() : null);
                        abhaData.setAuthenticationMode(record[3] != null ? record[3].toString() : null);
                        abhaData.setAbhaCreatedDate(record[4] != null ? record[4].toString() : null);

                        abhaMap.put(benRegId, abhaData);
                    }
                } catch (Exception e) {
                    logger.error("Error processing ABHA record: {}", e.getMessage());
                }
            }

            logger.debug("Processed {} unique ABHA records into map", abhaMap.size());

        } catch (Exception e) {
            logger.error("Error batch fetching ABHA details: {}", e.getMessage(), e);
        }

        return abhaMap;
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
     * Fetch single beneficiary WITH fresh ABHA data
     * Use this for real-time sync (create/update operations)
     */
    @Transactional(readOnly = true, timeout = 10)
    public BeneficiaryDocument getBeneficiaryWithAbhaDetails(BigInteger benRegId) {
        if (benRegId == null) {
            return null;
        }
        // Fetch beneficiary + ABHA in one call
        List<BigInteger> ids = List.of(benRegId);
        List<BeneficiaryDocument> results = getBeneficiariesBatch(ids);

        if (results.isEmpty()) {
            logger.warn("No beneficiary found for benRegId={}", benRegId);
            return null;
        }

        BeneficiaryDocument doc = results.get(0);

        // Log for debugging
        if (doc.getHealthID() != null || doc.getAbhaID() != null) {
            logger.info("Beneficiary has ABHA: benRegId={}, healthID={}, abhaID={}",
                    benRegId, doc.getHealthID(), doc.getAbhaID());
        } else {
            logger.debug("No ABHA details for benRegId={}", benRegId);
        }

        return doc;
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
            String beneficiaryID = getString(row[idx++]);
            if (beneficiaryID != null && !beneficiaryID.isEmpty()) {
                doc.setBenId(beneficiaryID);
            }
            doc.setBeneficiaryID(beneficiaryID);

            doc.setFirstName(getString(row[idx++]));
            doc.setMiddleName(getString(row[idx++]));   
            doc.setLastName(getString(row[idx++]));
            doc.setGenderID(getInteger(row[idx++]));
            doc.setGenderName(getString(row[idx++]));
            doc.setGender(doc.getGenderName());
            doc.setDOB(getDate(row[idx++]));
            doc.setAge(getInteger(row[idx++]));
            doc.setFatherName(getString(row[idx++]));
            doc.setSpouseName(getString(row[idx++]));

            doc.setMaritalStatusID(getInteger(row[idx++])); 
            doc.setMaritalStatusName(getString(row[idx++]));
            doc.setIsHIVPos(getString(row[idx++]));

            doc.setCreatedBy(getString(row[idx++]));
            doc.setCreatedDate(getDate(row[idx++]));
            doc.setLastModDate(getLong(row[idx++]));
            doc.setBenAccountID(getLong(row[idx++]));

            doc.setPhoneNum(getString(row[idx++]));
            doc.setFamilyID(getString(row[idx++]));

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

            doc.setPermStateID(getInteger(row[idx++]));
            doc.setPermStateName(getString(row[idx++]));
            doc.setPermDistrictID(getInteger(row[idx++]));
            doc.setPermDistrictName(getString(row[idx++]));
            doc.setPermBlockID(getInteger(row[idx++]));
            doc.setPermBlockName(getString(row[idx++]));
            doc.setPermVillageID(getInteger(row[idx++]));
            doc.setPermVillageName(getString(row[idx++]));

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
        if (value == null)
            return null;
        if (value instanceof Long)
            return (Long) value;
        if (value instanceof Integer)
            return ((Integer) value).longValue();
        if (value instanceof BigInteger)
            return ((BigInteger) value).longValue();
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer getInteger(Object value) {
        if (value == null)
            return null;
        if (value instanceof Integer)
            return (Integer) value;
        if (value instanceof Long)
            return ((Long) value).intValue();
        if (value instanceof BigInteger)
            return ((BigInteger) value).intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private java.util.Date getDate(Object value) {
        if (value == null)
            return null;
        if (value instanceof java.util.Date)
            return (java.util.Date) value;
        if (value instanceof java.sql.Timestamp)
            return new java.util.Date(((java.sql.Timestamp) value).getTime());
        if (value instanceof java.sql.Date)
            return new java.util.Date(((java.sql.Date) value).getTime());
        return null;
    }

    /**
     * Inner class to hold ABHA data
     */
    private static class AbhaData {
        private String healthID;
        private String healthIDNumber;
        private String authenticationMode;
        private String abhaCreatedDate;

        public String getHealthID() {
            return healthID;
        }

        public void setHealthID(String healthID) {
            this.healthID = healthID;
        }

        public String getHealthIDNumber() {
            return healthIDNumber;
        }

        public void setHealthIDNumber(String healthIDNumber) {
            this.healthIDNumber = healthIDNumber;
        }

        public String getAuthenticationMode() {
            return authenticationMode;
        }

        public void setAuthenticationMode(String authenticationMode) {
            this.authenticationMode = authenticationMode;
        }

        public String getAbhaCreatedDate() {
            return abhaCreatedDate;
        }

        public void setAbhaCreatedDate(String abhaCreatedDate) {
            this.abhaCreatedDate = abhaCreatedDate;
        }
    }

}