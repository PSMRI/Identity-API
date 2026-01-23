package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.repo.elasticsearch.SyncJobRepo;

@Service
public class BeneficiaryElasticsearchIndexService {

    private static final Logger logger = LoggerFactory.getLogger(BeneficiaryElasticsearchIndexService.class);
    private static final int BATCH_SIZE = 2000;
    private static final int ES_BULK_SIZE = 5000;
    private static final int STATUS_UPDATE_FREQUENCY = 5;

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private BeneficiaryTransactionHelper transactionalWrapper;

    @Autowired
    private BeneficiaryDocumentDataService dataService;

    @Autowired
    private SyncJobRepo syncJobRepository;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    /**
     * Start async full sync job with COMPLETE 38+ field data
     */
    @Async("elasticsearchSyncExecutor")
    public void syncAllBeneficiariesAsync(Long jobId, String triggeredBy) {
        logger.info("Starting ASYNC full sync with COMPLETE data: jobId={}", jobId);

        ElasticsearchSyncJob job = syncJobRepository.findByJobId(jobId)
                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));

        try {
            boolean isResume = false;
            int offset = 0;
            long processedCount = 0;
            long successCount = 0;
            long failureCount = 0;

            // Check if this is a resume from a previous run
            if (job.getStatus().equals("RUNNING") && job.getCurrentOffset() != null && job.getCurrentOffset() > 0) {
                isResume = true;
                offset = job.getCurrentOffset();
                processedCount = job.getProcessedRecords() != null ? job.getProcessedRecords() : 0;
                successCount = job.getSuccessCount() != null ? job.getSuccessCount() : 0;
                failureCount = job.getFailureCount() != null ? job.getFailureCount() : 0;

                logger.info("RESUMING SYNC from offset {} (processed: {}, success: {}, failed: {})",
                        offset, processedCount, successCount, failureCount);
            } else {
                job.setStatus("RUNNING");
                job.setStartedAt(new Timestamp(System.currentTimeMillis()));
                syncJobRepository.save(job);
            }

            long totalCount;
            if (job.getTotalRecords() != null && job.getTotalRecords() > 0) {
                totalCount = job.getTotalRecords();
                logger.info("Using cached total count: {}", totalCount);
            } else {
                totalCount = transactionalWrapper.countActiveBeneficiaries();
                job.setTotalRecords(totalCount);
                syncJobRepository.save(job);
                logger.info("Fetched total beneficiaries to sync: {}", totalCount);
            }

            if (totalCount == 0) {
                job.setStatus("COMPLETED");
                job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
                job.setErrorMessage("No beneficiaries found to sync");
                syncJobRepository.save(job);
                return;
            }

            List<BeneficiaryDocument> esBatch = new ArrayList<>();
            int batchCounter = offset / BATCH_SIZE;
            long startTime = isResume ? job.getStartedAt().getTime() : System.currentTimeMillis();
            long lastProgressUpdate = System.currentTimeMillis();
            int consecutiveErrors = 0;
            final int MAX_CONSECUTIVE_ERRORS = 5;

            // Process in batches
            while (offset < totalCount) {
                try {
                    logger.info("=== BATCH {} START: offset={}/{} ({:.1f}%) ===",
                            batchCounter + 1, offset, totalCount, (offset * 100.0 / totalCount));

                    logger.debug("Calling getBeneficiaryIdsBatch(offset={}, limit={})", offset, BATCH_SIZE);
                    List<Object[]> batchIds = transactionalWrapper.getBeneficiaryIdsBatch(offset, BATCH_SIZE);

                    logger.info("Retrieved {} IDs from database", batchIds != null ? batchIds.size() : 0);

                    if (batchIds == null || batchIds.isEmpty()) {
                        logger.warn("No more batches to process at offset {}", offset);
                        break;
                    }

                    // Reset consecutive error counter on successful fetch
                    consecutiveErrors = 0;

                    logger.debug("Converting {} IDs to BigInteger", batchIds.size());
                    List<BigInteger> benRegIds = batchIds.stream()
                            .map(arr -> toBigInteger(arr[0]))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    logger.info("Converted {} valid BigInteger IDs", benRegIds.size());

                    if (benRegIds.isEmpty()) {
                        logger.error("No valid IDs in batch at offset {} - ALL IDs FAILED CONVERSION", offset);
                        offset += BATCH_SIZE;
                        continue;
                    }

                    logger.info("Fetching complete data for {} beneficiaries...", benRegIds.size());
                    List<BeneficiaryDocument> documents = dataService.getBeneficiariesBatch(benRegIds);

                    logger.info("✓ Fetched {} complete documents for batch at offset {}", documents.size(), offset);

                    int docsAddedInThisBatch = 0;
                    for (BeneficiaryDocument doc : documents) {
                        try {
                            if (doc != null && doc.getBenId() != null) {
                                esBatch.add(doc);
                                docsAddedInThisBatch++;

                                if (esBatch.size() >= ES_BULK_SIZE) {
                                    logger.info("ES batch full ({} docs), indexing now...", esBatch.size());
                                    int indexed = bulkIndexDocuments(esBatch);
                                    successCount += indexed;
                                    failureCount += (esBatch.size() - indexed);
                                    processedCount += esBatch.size();

                                    logger.info("✓ Indexed {}/{} documents successfully", indexed, esBatch.size());
                                    esBatch.clear();
                                }
                            } else {
                                logger.warn("Skipping document - doc null: {}, benId null: {}",
                                        doc == null, doc != null ? (doc.getBenId() == null) : "N/A");
                                failureCount++;
                                processedCount++;
                            }
                        } catch (Exception e) {
                            logger.error("Error processing single document: {}", e.getMessage(), e);
                            failureCount++;
                            processedCount++;
                        }
                    }

                    logger.info("Added {} documents to ES batch in this iteration", docsAddedInThisBatch);

                    int notFetched = benRegIds.size() - documents.size();
                    if (notFetched > 0) {
                        failureCount += notFetched;
                        processedCount += notFetched;
                        logger.warn("{} beneficiaries not fetched from database", notFetched);
                    }

                    offset += BATCH_SIZE;
                    batchCounter++;

                    logger.info("=== BATCH {} END: Processed={}, Success={}, Failed={} ===",
                            batchCounter, processedCount, successCount, failureCount);

                    // Save progress every batch for resume capability
                    long now = System.currentTimeMillis();
                    if (batchCounter % STATUS_UPDATE_FREQUENCY == 0 || (now - lastProgressUpdate) > 30000) {
                        logger.info("Saving checkpoint for resume capability...");
                        updateJobProgress(job, processedCount, successCount, failureCount,
                                offset, totalCount, startTime);
                        lastProgressUpdate = now;
                    }

                    // Brief pause every 10 batches
                    if (batchCounter % 10 == 0) {
                        logger.debug("Pausing for 500ms after {} batches", batchCounter);
                        Thread.sleep(500);
                    }

                } catch (Exception e) {
                    consecutiveErrors++;
                    logger.error("!!! ERROR #{} in batch at offset {}: {} !!!",
                            consecutiveErrors, offset, e.getMessage(), e);
                    logger.error("Exception type: {}", e.getClass().getName());

                    // Save progress before handling error
                    job.setCurrentOffset(offset);
                    job.setProcessedRecords(processedCount);
                    job.setSuccessCount(successCount);
                    job.setFailureCount(failureCount);
                    syncJobRepository.save(job);

                    // If too many consecutive errors, mark job as STALLED for manual intervention
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        logger.error("TOO MANY CONSECUTIVE ERRORS ({}). Marking job as STALLED.", consecutiveErrors);
                        job.setStatus("STALLED");
                        job.setErrorMessage("Too many consecutive errors at offset " + offset + ": " + e.getMessage());
                        syncJobRepository.save(job);
                        return;
                    }

                    // Skip this batch and continue
                    offset += BATCH_SIZE;

                    // Exponential backoff: wait longer after each error
                    long waitTime = Math.min(10000, 1000 * (long) Math.pow(2, consecutiveErrors));
                    logger.info("Waiting {}ms before retry...", waitTime);
                    Thread.sleep(waitTime);
                }
            }

            // Index remaining documents
            if (!esBatch.isEmpty()) {
                logger.info("Indexing final batch of {} documents", esBatch.size());
                int indexed = bulkIndexDocuments(esBatch);
                successCount += indexed;
                failureCount += (esBatch.size() - indexed);
                processedCount += esBatch.size();
            }

            // Mark as COMPLETED
            job.setStatus("COMPLETED");
            job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
            job.setProcessedRecords(processedCount);
            job.setSuccessCount(successCount);
            job.setFailureCount(failureCount);
            job.setCurrentOffset((int) totalCount);

            long duration = System.currentTimeMillis() - startTime;
            job.setProcessingSpeed(processedCount / (duration / 1000.0));

            syncJobRepository.save(job);

            logger.info("Async sync job COMPLETED: jobId={}", jobId);
            logger.info("Total: {}, Processed: {}, Success: {}, Failed: {}",
                    totalCount, processedCount, successCount, failureCount);
            logger.info("All 38+ beneficiary fields synced to Elasticsearch!");

        } catch (Exception e) {
            logger.error("CRITICAL ERROR in async sync: jobId={}, error={}", jobId, e.getMessage(), e);

            job.setStatus("FAILED");
            job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
            job.setErrorMessage(e.getMessage());
            syncJobRepository.save(job);
        }
    }

    // Resume a stalled job
    public void resumeStalledJob(Long jobId) {
        logger.info("Attempting to resume stalled job: {}", jobId);

        ElasticsearchSyncJob job = syncJobRepository.findByJobId(jobId)
                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));

        if (!job.getStatus().equals("STALLED") && !job.getStatus().equals("RUNNING")) {
            throw new RuntimeException("Cannot resume job with status: " + job.getStatus());
        }

        // Reset error counter and continue from last checkpoint
        logger.info("Resuming from offset: {}", job.getCurrentOffset());
        syncAllBeneficiariesAsync(jobId, "AUTO_RESUME");
    }

    /**
     * Helper method to safely convert various numeric types to BigInteger
     * CRITICAL: Native SQL queries return Long, not BigInteger
     */
    private BigInteger toBigInteger(Object value) {
        if (value == null) {
            logger.warn("Attempted to convert null value to BigInteger");
            return null;
        }

        try {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof Long) {
                return BigInteger.valueOf((Long) value);
            }
            if (value instanceof Integer) {
                return BigInteger.valueOf(((Integer) value).longValue());
            }
            if (value instanceof Number) {
                return BigInteger.valueOf(((Number) value).longValue());
            }
            return new BigInteger(value.toString());
        } catch (NumberFormatException e) {
            logger.error("Cannot convert '{}' (type: {}) to BigInteger: {}",
                    value, value.getClass().getName(), e.getMessage());
            return null;
        }
    }

    private void updateJobProgress(ElasticsearchSyncJob job, long processed, long success,
            long failure, int offset, long total, long startTime) {
        job.setProcessedRecords(processed);
        job.setSuccessCount(success);
        job.setFailureCount(failure);
        job.setCurrentOffset(offset);

        long elapsedTime = System.currentTimeMillis() - startTime;
        double speed = elapsedTime > 0 ? processed / (elapsedTime / 1000.0) : 0;
        job.setProcessingSpeed(speed);

        if (speed > 0) {
            long remaining = total - processed;
            long estimatedSeconds = (long) (remaining / speed);
            job.setEstimatedTimeRemaining(estimatedSeconds);
        }

        syncJobRepository.save(job);

    }

    private int bulkIndexDocuments(List<BeneficiaryDocument> documents) {
        if (documents == null || documents.isEmpty()) {
            return 0;
        }

        try {
            BulkRequest.Builder br = new BulkRequest.Builder();

            for (BeneficiaryDocument doc : documents) {
                if (doc.getBenId() != null) {
                    br.operations(op -> op
                            .index(idx -> idx
                                    .index(beneficiaryIndex)
                                    .id(doc.getBenId())
                                    .document(doc)));
                }
            }

            BulkResponse result = esClient.bulk(br.build());
            int successCount = 0;

            if (result.errors()) {
                for (BulkResponseItem item : result.items()) {
                    if (item.error() == null) {
                        successCount++;
                    } else {
                        logger.error("ES indexing error for doc {}: {}",
                                item.id(), item.error().reason());
                    }
                }
            } else {
                successCount = documents.size();
            }

            logger.debug("Bulk indexed {} documents successfully", successCount);
            return successCount;

        } catch (Exception e) {
            logger.error("Error in bulk indexing: {}", e.getMessage(), e);
            return 0;
        }
    }
}