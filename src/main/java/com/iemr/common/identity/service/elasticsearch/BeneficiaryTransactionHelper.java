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