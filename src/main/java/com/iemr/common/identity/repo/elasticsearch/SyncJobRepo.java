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