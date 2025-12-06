package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;

/**
 * Optimized service to fetch beneficiary data in bulk
 * Uses native SQL with joins for maximum performance
 */
@Service
public class OptimizedBeneficiaryDataService {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedBeneficiaryDataService.class);

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Fetch multiple beneficiaries in one query using native SQL
     * This is MUCH faster than fetching one by one
     */
    @Transactional(readOnly = true, timeout = 30)
    public List<BeneficiariesDTO> getBeneficiariesBatch(List<BigInteger> benRegIds) {
        if (benRegIds == null || benRegIds.isEmpty()) {
            return new ArrayList<>();
        }

        try {
            // CORRECTED: Use exact column names from your tables
            String sql = 
                "SELECT " +
                "  m.BenRegId, " +
                "  m.BenMapId, " +
                "  d.FirstName, " +
                "  d.LastName, " +
                "  d.Gender, " +  // Changed from Gender to GenderName
                "  d.DOB, " +
                "  c.PreferredPhoneNum " +
                "FROM i_beneficiarymapping m " +
                "LEFT JOIN i_beneficiarydetails d ON m.BenDetailsId = d.BeneficiaryDetailsId " +
                "LEFT JOIN i_beneficiarycontacts c ON m.BenContactsId = c.BenContactsID " +
                "WHERE m.BenRegId IN :benRegIds " +
                "  AND m.deleted = false";

            Query query = entityManager.createNativeQuery(sql);
            query.setParameter("benRegIds", benRegIds);

            @SuppressWarnings("unchecked")
            List<Object[]> results = query.getResultList();

            logger.info("Fetched {} beneficiary records from database", results.size());

            List<BeneficiariesDTO> beneficiaries = new ArrayList<>();

            for (Object[] row : results) {
                try {
                    BeneficiariesDTO dto = new BeneficiariesDTO();

                    // BenRegId (column 0) - Handle both Long and BigInteger
                    if (row[0] != null) {
                        BigInteger benRegId = convertToBigInteger(row[0]);
                        dto.setBenRegId(benRegId);
                        dto.setBenId(benRegId); // Use benRegId as benId
                    }

                    // BenMapId (column 1) - Handle both Long and BigInteger
                    if (row[1] != null) {
                        dto.setBenMapId(convertToBigInteger(row[1]));
                    }

                    // Create BenDetailDTO
                    BenDetailDTO detailDTO = new BenDetailDTO();
                    detailDTO.setFirstName(row[2] != null ? row[2].toString() : null);
                    detailDTO.setLastName(row[3] != null ? row[3].toString() : null);
                    detailDTO.setGender(row[4] != null ? row[4].toString() : null);

                    // Calculate age from DOB (column 5)
                    if (row[5] != null) {
                        try {
                            java.sql.Timestamp dob = (java.sql.Timestamp) row[5];
                            java.time.LocalDate birthDate = dob.toLocalDateTime().toLocalDate();
                            java.time.LocalDate now = java.time.LocalDate.now();
                            int age = java.time.Period.between(birthDate, now).getYears();
                            detailDTO.setBeneficiaryAge(age);
                        } catch (Exception e) {
                            logger.debug("Error calculating age: {}", e.getMessage());
                        }
                    }

                    dto.setBeneficiaryDetails(detailDTO);

                    // Phone number (column 6)
                    dto.setPreferredPhoneNum(row[6] != null ? row[6].toString() : null);

                    beneficiaries.add(dto);

                } catch (Exception e) {
                    logger.error("Error parsing row: {}", e.getMessage(), e);
                }
            }

            logger.debug("Converted {} beneficiaries to DTOs", beneficiaries.size());
            return beneficiaries;

        } catch (Exception e) {
            logger.error("Error fetching beneficiaries batch: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Single beneficiary fetch (for backward compatibility)
     */
    @Transactional(readOnly = true, timeout = 10)
    public BeneficiariesDTO getBeneficiaryFromDatabase(BigInteger benRegId) {
        List<BigInteger> ids = new ArrayList<>();
        ids.add(benRegId);
        
        List<BeneficiariesDTO> results = getBeneficiariesBatch(ids);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * Helper method to convert Object to BigInteger
     * Handles both Long and BigInteger types from database
     */
    private BigInteger convertToBigInteger(Object value) {
        if (value == null) {
            return null;
        }
        
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Long) {
            return BigInteger.valueOf((Long) value);
        } else if (value instanceof Integer) {
            return BigInteger.valueOf((Integer) value);
        } else if (value instanceof String) {
            return new BigInteger((String) value);
        } else {
            logger.warn("Unexpected type for BigInteger conversion: {}", value.getClass().getName());
            return BigInteger.valueOf(((Number) value).longValue());
        }
    }
}