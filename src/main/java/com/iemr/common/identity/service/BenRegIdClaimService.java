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
package com.iemr.common.identity.service;

import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.domain.MBeneficiaryregidmapping;
import com.iemr.common.identity.repo.BenRegIdMappingRepo;

/**
 * Handles atomic beneficiary registration ID claiming.
 *
 * Uses SELECT ... FOR UPDATE SKIP LOCKED so that each application instance
 * obtains a distinct, exclusive row. Multiple servers sharing the same database
 * will never receive the same BenRegId, preventing
 * SQLIntegrityConstraintViolationException duplicate-key errors that occurred
 * with the previous in-memory ArrayDeque queue approach.
 *
 * REQUIRES_NEW propagation ensures the SELECT + UPDATE happens in its own
 * short-lived transaction, releasing the row lock immediately after the ID is
 * marked reserved — keeping lock contention to a minimum.
 */
@Service
public class BenRegIdClaimService {

    private static final Logger logger = LoggerFactory.getLogger(BenRegIdClaimService.class);

    @Autowired
    private BenRegIdMappingRepo regIdRepo;

    /**
     * Atomically claims the next available registration ID.
     *
     * <ol>
     *   <li>Opens a brand-new transaction (REQUIRES_NEW).</li>
     *   <li>Executes SELECT … FOR UPDATE SKIP LOCKED to lock exactly one row.
     *       Concurrent callers on other servers/threads skip the locked row and
     *       get the next one — no two callers ever see the same row.</li>
     *   <li>Marks the row {@code reserved = true} and flushes it within the same
     *       transaction so the change is visible to other connections the moment
     *       this method returns.</li>
     * </ol>
     *
     * @return the reserved {@link MBeneficiaryregidmapping} with {@code reserved=true}
     * @throws IllegalStateException if the ID pool is exhausted
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public MBeneficiaryregidmapping claimNextAvailableRegId() {
        MBeneficiaryregidmapping regMap = regIdRepo.findAndLockNextAvailable();
        if (regMap == null) {
            throw new IllegalStateException(
                    "No available registration IDs in the pool. "
                    + "Please contact the system administrator to import more IDs.");
        }
        if (regMap.getCreatedDate() == null) {
            regMap.setCreatedDate(new Timestamp(System.currentTimeMillis()));
        }
        regMap.setReserved(true);
        regMap = regIdRepo.save(regMap);
        logger.info("BenRegIdClaimService: claimed BenRegId={}", regMap.getBenRegId());
        return regMap;
    }
}
