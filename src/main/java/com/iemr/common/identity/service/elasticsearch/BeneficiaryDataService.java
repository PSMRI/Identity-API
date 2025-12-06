package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.repo.BenMappingRepo;

/**
 * Service to fetch beneficiary data directly from database
 * Used for Elasticsearch sync to avoid circular dependencies
 */
@Service
public class BeneficiaryDataService {

    private static final Logger logger = LoggerFactory.getLogger(BeneficiaryDataService.class);

    @Autowired
    private BenMappingRepo mappingRepo;

    /**
     * Fetch beneficiary data directly from database by benRegId
     * This bypasses any Elasticsearch caching to get fresh database data
     * 
     * @param benRegId The beneficiary registration ID
     * @return BeneficiariesDTO or null if not found
     */
    public BeneficiariesDTO getBeneficiaryFromDatabase(BigInteger benRegId) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                logger.debug("Fetching beneficiary from database: benRegId={}, attempt={}", benRegId, retryCount + 1);
                
                // Fetch with details
                MBeneficiarymapping mapping = mappingRepo.findByBenRegIdWithDetails(benRegId);
                
                if (mapping == null) {
                    logger.warn("Beneficiary mapping not found: benRegId={}", benRegId);
                    return null;
                }
                
                // Convert to DTO
                BeneficiariesDTO dto = convertToDTO(mapping);
                
                logger.debug("Successfully fetched beneficiary: benRegId={}", benRegId);
                return dto;
                
            } catch (org.springframework.orm.jpa.JpaSystemException e) {
                retryCount++;
                logger.warn("Database connection error for benRegId={}, attempt {}/{}: {}", 
                    benRegId, retryCount, maxRetries, e.getMessage());
                
                if (retryCount >= maxRetries) {
                    logger.error("Max retries reached for benRegId={}", benRegId);
                    return null;
                }
                
                // Wait before retry
                try {
                    Thread.sleep(1000 * retryCount); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                
            } catch (Exception e) {
                logger.error("Error fetching beneficiary from database: benRegId={}, error={}", 
                    benRegId, e.getMessage(), e);
                return null;
            }
        }
        
        return null;
    }

    /**
     * Convert MBeneficiarymapping entity to BeneficiariesDTO
     */
    private BeneficiariesDTO convertToDTO(MBeneficiarymapping mapping) {
        BeneficiariesDTO dto = new BeneficiariesDTO();
        
        try {
            // Basic IDs
            dto.setBenRegId(mapping.getBenRegId());
            dto.setBenMapId(mapping.getBenMapId());
            
            // Use benRegId as benId if benId is not available
            if (mapping.getBenRegId() != null) {
                dto.setBenId(mapping.getBenRegId());
            }
            
            // Phone number from contact
            if (mapping.getMBeneficiarycontact() != null) {
                dto.setPreferredPhoneNum(mapping.getMBeneficiarycontact().getPreferredPhoneNum());
            }
            
            // Beneficiary details
            if (mapping.getMBeneficiarydetail() != null) {
                BenDetailDTO detailDTO = new BenDetailDTO();
                
                detailDTO.setFirstName(mapping.getMBeneficiarydetail().getFirstName());
                detailDTO.setLastName(mapping.getMBeneficiarydetail().getLastName());
                // detailDTO.setGender(mapping.getMBeneficiarydetail().getGenderName());
                
                // Calculate age if DOB is available
                if (mapping.getMBeneficiarydetail().getDob() != null) {
                    detailDTO.setBeneficiaryAge(calculateAge(mapping.getMBeneficiarydetail().getDob()));
                }
                
                dto.setBeneficiaryDetails(detailDTO);
            }
            
            logger.debug("Successfully converted mapping to DTO: benRegId={}", mapping.getBenRegId());
            
        } catch (Exception e) {
            logger.error("Error converting mapping to DTO: {}", e.getMessage(), e);
        }
        
        return dto;
    }
    
    /**
     * Calculate age from date of birth
     */
    private Integer calculateAge(java.sql.Timestamp dob) {
        try {
            if (dob == null) {
                return null;
            }
            
            java.time.LocalDate birthDate = dob.toLocalDateTime().toLocalDate();
            java.time.LocalDate now = java.time.LocalDate.now();
            
            return java.time.Period.between(birthDate, now).getYears();
            
        } catch (Exception e) {
            logger.error("Error calculating age: {}", e.getMessage());
            return null;
        }
    }
}