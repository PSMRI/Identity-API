package com.iemr.common.identity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.service.elasticsearch.SyncJobService;

/**
 * Scheduled jobs for Elasticsearch sync
 * 
 * To enable scheduled sync, set:
 * elasticsearch.sync.scheduled.enabled=true in application.properties
 */
@Component
public class ScheduledSyncJob {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledSyncJob.class);

    @Autowired
    private SyncJobService syncJobService;

    @Value("${elasticsearch.sync.scheduled.enabled:true}")
    private boolean scheduledSyncEnabled;

    /**
     * Run full sync every day at 2 AM
     * Cron: second, minute, hour, day, month, weekday
     */
    @Scheduled(cron = "${elasticsearch.sync.scheduled.cron:0 0 2 * * ?}")
    public void scheduledFullSync() {
        if (!scheduledSyncEnabled) {
            logger.debug("Scheduled sync is disabled");
            return;
        }

        logger.info("========================================");
        logger.info("Starting scheduled full sync job");
        logger.info("========================================");

        try {
            // Check if there's already a sync running
            if (syncJobService.isFullSyncRunning()) {
                logger.warn("Full sync already running. Skipping scheduled sync.");
                return;
            }

            // Start async sync
            ElasticsearchSyncJob job = syncJobService.startFullSyncJob("SCHEDULER");
            logger.info("Scheduled sync job started: jobId={}", job.getJobId());

        } catch (Exception e) {
            logger.error("Error starting scheduled sync: {}", e.getMessage(), e);
        }
    }

    /**
     * Clean up old completed jobs (keep last 30 days)
     * Runs every Sunday at 3 AM
     */
    @Scheduled(cron = "0 0 3 * * SUN")
    public void cleanupOldJobs() {
        if (!scheduledSyncEnabled) {
            return;
        }

        logger.info("Running cleanup of old sync jobs...");
        
        // TODO: Implement cleanup logic
        // Delete jobs older than 30 days with status COMPLETED or FAILED
    }
}