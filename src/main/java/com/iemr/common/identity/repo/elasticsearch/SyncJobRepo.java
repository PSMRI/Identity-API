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

package com.iemr.common.identity.repo.elasticsearch;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;

@Repository
public interface SyncJobRepo extends JpaRepository<ElasticsearchSyncJob, Long> {

    /**
     * Find all active (running or pending) jobs
     */
    @Query("SELECT j FROM ElasticsearchSyncJob j WHERE j.status IN ('RUNNING', 'PENDING') ORDER BY j.createdDate DESC")
    List<ElasticsearchSyncJob> findActiveJobs();

    /**
     * Check if there's any active full sync job
     */
    @Query("SELECT COUNT(j) > 0 FROM ElasticsearchSyncJob j WHERE j.jobType = 'FULL_SYNC' AND j.status IN ('RUNNING', 'PENDING')")
    boolean hasActiveFullSyncJob();

    /**
     * Find latest job of a specific type
     */
    @Query("SELECT j FROM ElasticsearchSyncJob j WHERE j.jobType = :jobType ORDER BY j.createdDate DESC")
    List<ElasticsearchSyncJob> findLatestJobsByType(String jobType);

    /**
     * Find recent jobs (last 10)
     */
    @Query("SELECT j FROM ElasticsearchSyncJob j ORDER BY j.createdDate DESC")
    List<ElasticsearchSyncJob> findRecentJobs();
    
    /**
     * Find job by ID
     */
    Optional<ElasticsearchSyncJob> findByJobId(Long jobId);
}