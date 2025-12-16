package com.iemr.common.identity.controller.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchSyncService;
import com.iemr.common.identity.service.elasticsearch.SyncJobService;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchSyncService.SyncStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import com.iemr.common.identity.utils.response.OutputResponse;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchIndexingService;

/**
 * Controller to manage Elasticsearch synchronization operations
 * Supports both synchronous and asynchronous sync jobs
 */
@RestController
@RequestMapping("/elasticsearch")
public class ElasticsearchSyncController {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSyncController.class);

    @Autowired
    private ElasticsearchSyncService syncService;
    
    @Autowired
    private SyncJobService syncJobService;
    
    @Autowired
    private BenMappingRepo mappingRepo;

    @Autowired
    private ElasticsearchIndexingService indexingService;

    /**
     * Start async full sync (RECOMMENDED for millions of records)
     * Returns immediately with job ID for tracking
     * 
     * Usage: POST http://localhost:8080/elasticsearch/sync/start
     */
    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startAsyncFullSync(
            @RequestParam(required = false, defaultValue = "API") String triggeredBy) {
        
        logger.info("Received request to start ASYNC full sync");
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            ElasticsearchSyncJob job = syncJobService.startFullSyncJob(triggeredBy);
            
            response.put("status", "success");
            response.put("message", "Sync job started in background");
            response.put("jobId", job.getJobId());
            response.put("jobStatus", job.getStatus());
            response.put("checkStatusUrl", "/elasticsearch/status/" + job.getJobId());
            
            return ResponseEntity.ok(response);
            
        } catch (RuntimeException e) {
            logger.error("Error starting async sync: {}", e.getMessage());
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
            
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", "Unexpected error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Get job status by ID
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/status/1
     */
    @GetMapping("/status/{jobId}")
    public ResponseEntity<Map<String, Object>> getAsyncJobStatus(@PathVariable Long jobId) {
        logger.info("Checking status for job: {}", jobId);
        
        try {
            ElasticsearchSyncJob job = syncJobService.getJobStatus(jobId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobId", job.getJobId());
            response.put("jobType", job.getJobType());
            response.put("status", job.getStatus());
            response.put("totalRecords", job.getTotalRecords());
            response.put("processedRecords", job.getProcessedRecords());
            response.put("successCount", job.getSuccessCount());
            response.put("failureCount", job.getFailureCount());
            response.put("progressPercentage", String.format("%.2f", job.getProgressPercentage()));
            response.put("processingSpeed", job.getProcessingSpeed());
            response.put("estimatedTimeRemaining", job.getEstimatedTimeRemaining());
            response.put("startedAt", job.getStartedAt());
            response.put("completedAt", job.getCompletedAt());
            response.put("errorMessage", job.getErrorMessage());
            
            return ResponseEntity.ok(response);
            
        } catch (RuntimeException e) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
    }

    /**
     * Get all active jobs
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/active
     */
    @GetMapping("/active")
    public ResponseEntity<List<ElasticsearchSyncJob>> getActiveJobs() {
        logger.info("Fetching active jobs");
        return ResponseEntity.ok(syncJobService.getActiveJobs());
    }

    /**
     * Get recent jobs
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/recent
     */
    @GetMapping("/recent")
    public ResponseEntity<List<ElasticsearchSyncJob>> getRecentJobs() {
        logger.info("Fetching recent jobs");
        return ResponseEntity.ok(syncJobService.getRecentJobs());
    }

    /**
     * Resume a failed job
     * 
     * Usage: POST http://localhost:8080/elasticsearch/sync/resume/1
     */
    @PostMapping("/resume/{jobId}")
    public ResponseEntity<Map<String, Object>> resumeJob(
            @PathVariable Long jobId,
            @RequestParam(required = false, defaultValue = "API") String triggeredBy) {
        
        logger.info("Resuming job: {}", jobId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            ElasticsearchSyncJob job = syncJobService.resumeJob(jobId, triggeredBy);
            
            response.put("status", "success");
            response.put("message", "Job resumed");
            response.put("jobId", job.getJobId());
            response.put("resumedFromOffset", job.getCurrentOffset());
            
            return ResponseEntity.ok(response);
            
        } catch (RuntimeException e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    /**
     * Cancel a running job
     * 
     * Usage: POST http://localhost:8080/elasticsearch/sync/cancel/1
     */
    @PostMapping("/cancel/{jobId}")
    public ResponseEntity<Map<String, Object>> cancelJob(@PathVariable Long jobId) {
        logger.info("Cancelling job: {}", jobId);
        
        Map<String, Object> response = new HashMap<>();
        boolean cancelled = syncJobService.cancelJob(jobId);
        
        if (cancelled) {
            response.put("status", "success");
            response.put("message", "Job cancelled");
            return ResponseEntity.ok(response);
        } else {
            response.put("status", "error");
            response.put("message", "Could not cancel job. It may not be active.");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

    /**
     * LEGACY: Synchronous full sync (NOT recommended for large datasets)
     * Use /start instead
     * 
     * Usage: POST http://localhost:8080/elasticsearch/sync/all
     */
    @PostMapping("/all")
    public ResponseEntity<Map<String, Object>> syncAllBeneficiaries() {
        logger.warn("LEGACY sync endpoint called. Consider using /start instead.");
        logger.info("Received request to sync all beneficiaries (BLOCKING)");
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            ElasticsearchSyncService.SyncResult result = syncService.syncAllBeneficiaries();
            
            response.put("status", "completed");
            response.put("successCount", result.getSuccessCount());
            response.put("failureCount", result.getFailureCount());
            response.put("error", result.getError());
            response.put("warning", "This is a blocking operation. For large datasets, use /start");
            
            if (result.getError() != null) {
                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).body(response);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error in sync all endpoint: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Sync a single beneficiary by BenRegId
     * 
     * Usage: POST http://localhost:8080/elasticsearch/sync/single/123456
     */
    @PostMapping("/single/{benRegId}")
    public ResponseEntity<Map<String, Object>> syncSingleBeneficiary(
            @PathVariable String benRegId) {
        logger.info("Received request to sync single beneficiary: {}", benRegId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean success = syncService.syncSingleBeneficiary(benRegId);
            
            response.put("status", success ? "success" : "failed");
            response.put("benRegId", benRegId);
            response.put("synced", success);
            
            if (!success) {
                response.put("message", "Beneficiary not found in database or sync failed. Check logs for details.");
            } else {
                response.put("message", "Beneficiary successfully synced to Elasticsearch");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error syncing single beneficiary: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("benRegId", benRegId);
            response.put("synced", false);
            response.put("message", "Exception occurred: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Check sync status - compare DB count vs ES count
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/status
     */
    @GetMapping("/status")
    public ResponseEntity<SyncStatus> checkSyncStatus() {
        logger.info("Received request to check sync status");
        
        try {
            SyncStatus status = syncService.checkSyncStatus();
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            logger.error("Error checking sync status: {}", e.getMessage(), e);
            SyncStatus errorStatus = new SyncStatus();
            errorStatus.setError(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorStatus);
        }
    }

    /**
     * Health check endpoint
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Elasticsearch Sync Service");
        response.put("asyncJobsRunning", syncJobService.isFullSyncRunning());
        response.put("activeJobs", syncJobService.getActiveJobs().size());
        return ResponseEntity.ok(response);
    }
    
    /**
     * Debug endpoint to check if a beneficiary exists in database
     * 
     * Usage: GET http://localhost:8080/elasticsearch/sync/debug/check/123456
     */
    @GetMapping("/debug/check/{benRegId}")
    public ResponseEntity<Map<String, Object>> checkBeneficiaryExists(
            @PathVariable String benRegId) {
        logger.info("Checking if beneficiary exists: {}", benRegId);
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            java.math.BigInteger benRegIdBig = new java.math.BigInteger(benRegId);
            
            boolean exists = mappingRepo.existsByBenRegId(benRegIdBig);
            
            response.put("benRegId", benRegId);
            response.put("existsInDatabase", exists);
            
            if (exists) {
                response.put("message", "Beneficiary found in database");
                
                MBeneficiarymapping mapping = 
                    mappingRepo.findByBenRegId(benRegIdBig);
                
                if (mapping != null) {
                    response.put("benMapId", mapping.getBenMapId());
                    response.put("deleted", mapping.getDeleted());
                    response.put("hasDetails", mapping.getMBeneficiarydetail() != null);
                    response.put("hasContact", mapping.getMBeneficiarycontact() != null);
                }
            } else {
                response.put("message", "Beneficiary NOT found in database");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error checking beneficiary: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

     /**
     * Create or recreate the Elasticsearch index with proper mapping
     * This will DELETE the existing index and create a new one
     * 
     * POST /elasticsearch/index/create
     */
    @PostMapping("/index/create")
    public ResponseEntity<OutputResponse> createIndex() {
        logger.info("API: Create Elasticsearch index request received");
        OutputResponse response = new OutputResponse();

        try {
            indexingService.createIndexWithMapping();
            
            response.setResponse("Index created successfully. Ready for data sync.");
            logger.info("Index created successfully");
            
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error creating index: {}", e.getMessage(), e);
            response.setError(5000, "Error creating index: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    /**
     * Recreate index and immediately start syncing data
     * This is a convenience endpoint that does both operations
     * 
     * POST /elasticsearch/index/recreate-and-sync
     */
    @PostMapping("/index/recreate-and-sync")
    public ResponseEntity<OutputResponse> recreateAndSync() {
        logger.info("API: Recreate index and sync request received");
        OutputResponse response = new OutputResponse();

        try {
            // Step 1: Recreate index
            logger.info("Step 1: Recreating index...");
            indexingService.createIndexWithMapping();
            logger.info("Index recreated successfully");

            // Step 2: Start sync
            logger.info("Step 2: Starting data sync...");
            Map<String, Integer> syncResult = indexingService.indexAllBeneficiaries();
            
            response.setResponse("Index recreated and sync started. Success: " + 
                syncResult.get("success") + ", Failed: " + syncResult.get("failed"));
            
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error in recreate and sync: {}", e.getMessage(), e);
            response.setError(5000, "Error: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    /**
     * Get information about the current index
     * Shows mapping, document count, etc.
     * 
     * GET /elasticsearch/index/info
     */
    @GetMapping("/index/info")
    public ResponseEntity<OutputResponse> getIndexInfo() {
        logger.info("API: Get index info request received");
        OutputResponse response = new OutputResponse();

        try {
            // You can add code here to get index stats using esClient
            response.setResponse("Index info endpoint - implementation pending");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting index info: {}", e.getMessage(), e);
            response.setError(5000, "Error: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
}