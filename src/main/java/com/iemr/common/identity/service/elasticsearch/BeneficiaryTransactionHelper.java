package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.repo.BenMappingRepo;

/**
 * Wrapper service to handle database operations with proper transaction management
 * This prevents connection timeout issues during long-running sync operations
 */
@Service
public class BeneficiaryTransactionHelper {

    private static final Logger logger = LoggerFactory.getLogger(BeneficiaryTransactionHelper.class);

    @Autowired
    private BenMappingRepo mappingRepo;

    /**
     * Get beneficiary IDs in a new transaction
     * This ensures connection is fresh for each batch
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, timeout = 30)
    public List<Object[]> getBeneficiaryIdsBatch(int offset, int limit) {
        try {
            return mappingRepo.getBeneficiaryIdsBatch(offset, limit);
        } catch (Exception e) {
            logger.error("Error fetching batch: offset={}, limit={}, error={}", 
                offset, limit, e.getMessage());
            throw e;
        }
    }

    /**
     * Count beneficiaries in a new transaction
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, timeout = 30)
    public long countActiveBeneficiaries() {
        try {
            return mappingRepo.countActiveBeneficiaries();
        } catch (Exception e) {
            logger.error("Error counting beneficiaries: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Check if beneficiary exists in a new transaction
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, timeout = 10)
    public boolean existsByBenRegId(BigInteger benRegId) {
        try {
            return mappingRepo.existsByBenRegId(benRegId);
        } catch (Exception e) {
            logger.error("Error checking existence for benRegId={}: {}", benRegId, e.getMessage());
            throw e;
        }
    }
}