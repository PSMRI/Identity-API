package com.iemr.common.identity.service.elasticsearch;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.repo.elasticsearch.SyncJobRepo;

/**
 * Service to manage Elasticsearch sync jobs
 */
@Service
public class SyncJobService {

    private static final Logger logger = LoggerFactory.getLogger(SyncJobService.class);

    @Autowired
    private SyncJobRepo syncJobRepository;

    @Autowired
    private BeneficiaryElasticsearchIndexService syncService;

    /**
     * Start a new full sync job
     * Returns immediately with job ID
     */
    public ElasticsearchSyncJob startFullSyncJob(String triggeredBy) {
        // Check if there's already an active full sync job
        if (syncJobRepository.hasActiveFullSyncJob()) {
            throw new RuntimeException("A full sync job is already running. Please wait for it to complete.");
        }

        // Create new job
        ElasticsearchSyncJob job = new ElasticsearchSyncJob();
        job.setJobType("FULL_SYNC");
        job.setStatus("PENDING");
        job.setTriggeredBy(triggeredBy);
        job.setProcessedRecords(0L);
        job.setSuccessCount(0L);
        job.setFailureCount(0L);
        job.setCurrentOffset(0);

        // Save job to database
        job = syncJobRepository.save(job);

        logger.info("Created new full sync job");

        // Start async processing
        syncService.syncAllBeneficiariesAsync(job.getJobId(), triggeredBy);

        return job;
    }

    /**
     * Resume a failed job from where it left off
     */
    public ElasticsearchSyncJob resumeJob(Long jobId, String triggeredBy) {
        ElasticsearchSyncJob job = syncJobRepository.findByJobId(jobId)
            .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));

        if (!"FAILED".equals(job.getStatus())) {
            throw new RuntimeException("Can only resume FAILED jobs. Current status: " + job.getStatus());
        }

        logger.info("Resuming job: jobId={}, from offset={}", jobId, job.getCurrentOffset());

        job.setStatus("PENDING");
        job.setTriggeredBy(triggeredBy);
        job = syncJobRepository.save(job);

        // Restart async processing from last offset
        syncService.syncAllBeneficiariesAsync(job.getJobId(), triggeredBy);

        return job;
    }

    /**
     * Cancel a running job
     */
    public boolean cancelJob(Long jobId) {
        Optional<ElasticsearchSyncJob> jobOpt = syncJobRepository.findByJobId(jobId);
        
        if (jobOpt.isEmpty()) {
            return false;
        }

        ElasticsearchSyncJob job = jobOpt.get();
        
        if (!job.isActive()) {
            logger.warn("Cannot cancel job that is not active: jobId={}, status={}", jobId, job.getStatus());
            return false;
        }

        // Mark as cancelled (the async thread will check this periodically)
        job.setStatus("CANCELLED");
        job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
        syncJobRepository.save(job);

        logger.info("Job cancelled: jobId={}", jobId);
        return true;
    }

    /**
     * Get job status by ID
     */
    public ElasticsearchSyncJob getJobStatus(Long jobId) {
        return syncJobRepository.findByJobId(jobId)
            .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));
    }

    /**
     * Get all active jobs
     */
    public List<ElasticsearchSyncJob> getActiveJobs() {
        return syncJobRepository.findActiveJobs();
    }

    /**
     * Get recent jobs (last 10)
     */
    public List<ElasticsearchSyncJob> getRecentJobs() {
        return syncJobRepository.findRecentJobs();
    }

    /**
     * Check if any full sync is currently running
     */
    public boolean isFullSyncRunning() {
        return syncJobRepository.hasActiveFullSyncJob();
    }

    /**
     * Get latest job of specific type
     */
    public ElasticsearchSyncJob getLatestJobByType(String jobType) {
        List<ElasticsearchSyncJob> jobs = syncJobRepository.findLatestJobsByType(jobType);
        return jobs.isEmpty() ? null : jobs.get(0);
    }
}