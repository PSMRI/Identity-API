package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.repo.elasticsearch.SyncJobRepo;
import org.springframework.beans.factory.annotation.Value;

/**
 * Async service for Elasticsearch sync operations
 * Runs sync jobs in background without blocking API calls
 */
@Service
public class AsyncElasticsearchSyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncElasticsearchSyncService.class);
    private static final int BATCH_SIZE = 100;
    private static final int ES_BULK_SIZE = 50;
    private static final int STATUS_UPDATE_FREQUENCY = 10; // Update status every 10 batches

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private TransactionalSyncWrapper transactionalWrapper;

    @Autowired
    private BeneficiaryDataService beneficiaryDataService;
    
    @Autowired
    private OptimizedBeneficiaryDataService optimizedDataService;

    @Autowired
    private SyncJobRepo syncJobRepository;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    /**
     * Start async full sync job
     * Runs in background thread from elasticsearchSyncExecutor pool
     */
    @Async("elasticsearchSyncExecutor")
    public void syncAllBeneficiariesAsync(Long jobId, String triggeredBy) {
        logger.info("========================================");
        logger.info("Starting ASYNC full sync job: jobId={}", jobId);
        logger.info("========================================");

        ElasticsearchSyncJob job = syncJobRepository.findByJobId(jobId)
            .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));

        try {
            // Update job status to RUNNING
            job.setStatus("RUNNING");
            job.setStartedAt(new Timestamp(System.currentTimeMillis()));
            syncJobRepository.save(job);

            // Get total count
            long totalCount = transactionalWrapper.countActiveBeneficiaries();
            job.setTotalRecords(totalCount);
            syncJobRepository.save(job);

            logger.info("Total beneficiaries to sync: {}", totalCount);

            if (totalCount == 0) {
                job.setStatus("COMPLETED");
                job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
                job.setErrorMessage("No beneficiaries found to sync");
                syncJobRepository.save(job);
                return;
            }

            int offset = job.getCurrentOffset() != null ? job.getCurrentOffset() : 0;
            long processedCount = job.getProcessedRecords() != null ? job.getProcessedRecords() : 0;
            long successCount = job.getSuccessCount() != null ? job.getSuccessCount() : 0;
            long failureCount = job.getFailureCount() != null ? job.getFailureCount() : 0;
            
            List<BeneficiaryDocument> esBatch = new ArrayList<>();
            int batchCounter = 0;
            long startTime = System.currentTimeMillis();

            // Process in batches
            while (offset < totalCount) {
                try {
                    List<Object[]> batchIds = transactionalWrapper.getBeneficiaryIdsBatch(offset, BATCH_SIZE);

                    if (batchIds == null || batchIds.isEmpty()) {
                        break;
                    }

                    // Extract benRegIds for batch fetch
                    List<BigInteger> benRegIds = batchIds.stream()
                        .map(arr -> (BigInteger) arr[0])
                        .collect(java.util.stream.Collectors.toList());

                    // Fetch ALL beneficiaries in this batch in ONE query
                    List<BeneficiariesDTO> benDTOs = optimizedDataService.getBeneficiariesBatch(benRegIds);
                    
                    logger.debug("Fetched {} beneficiaries in single batch query", benDTOs.size());

                    // Process each beneficiary
                    for (BeneficiariesDTO benDTO : benDTOs) {
                        try {
                            if (benDTO != null) {
                                BeneficiaryDocument doc = convertToDocument(benDTO);
                                if (doc != null && doc.getBenId() != null) {
                                    esBatch.add(doc);

                                    if (esBatch.size() >= ES_BULK_SIZE) {
                                        int indexed = bulkIndexDocuments(esBatch);
                                        successCount += indexed;
                                        failureCount += (esBatch.size() - indexed);
                                        processedCount += esBatch.size();
                                        esBatch.clear();
                                    }
                                } else {
                                    failureCount++;
                                    processedCount++;
                                }
                            } else {
                                failureCount++;
                                processedCount++;
                            }
                        } catch (Exception e) {
                            logger.error("Error processing beneficiary: {}", e.getMessage());
                            failureCount++;
                            processedCount++;
                        }
                    }
                    
                    // Account for any beneficiaries that weren't returned
                    int notFetched = batchIds.size() - benDTOs.size();
                    if (notFetched > 0) {
                        failureCount += notFetched;
                        processedCount += notFetched;
                        logger.warn("{} beneficiaries not fetched from database", notFetched);
                    }

                    offset += BATCH_SIZE;
                    batchCounter++;

                    // Update job status periodically
                    if (batchCounter % STATUS_UPDATE_FREQUENCY == 0) {
                        updateJobProgress(job, processedCount, successCount, failureCount, 
                            offset, totalCount, startTime);
                    }

                    // Small pause every 10 batches (not every 5)
                    if (batchCounter % 10 == 0) {
                        Thread.sleep(500); // Reduced to 500ms
                    }

                } catch (Exception e) {
                    logger.error("Error processing batch at offset {}: {}", offset, e.getMessage());
                    
                    // Save current progress before potentially failing
                    job.setCurrentOffset(offset);
                    job.setProcessedRecords(processedCount);
                    job.setSuccessCount(successCount);
                    job.setFailureCount(failureCount);
                    syncJobRepository.save(job);
                    
                    // Wait before retrying
                    Thread.sleep(2000);
                }
            }

            // Index remaining documents
            if (!esBatch.isEmpty()) {
                int indexed = bulkIndexDocuments(esBatch);
                successCount += indexed;
                failureCount += (esBatch.size() - indexed);
                processedCount += esBatch.size();
            }

            // Mark job as completed
            job.setStatus("COMPLETED");
            job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
            job.setProcessedRecords(processedCount);
            job.setSuccessCount(successCount);
            job.setFailureCount(failureCount);
            job.setCurrentOffset((int) totalCount);
            
            long duration = System.currentTimeMillis() - startTime;
            job.setProcessingSpeed(processedCount / (duration / 1000.0));
            
            syncJobRepository.save(job);

            logger.info("========================================");
            logger.info("Async sync job COMPLETED: jobId={}", jobId);
            logger.info("Processed: {}, Success: {}, Failed: {}", processedCount, successCount, failureCount);
            logger.info("========================================");

        } catch (Exception e) {
            logger.error("========================================");
            logger.error("CRITICAL ERROR in async sync job: jobId={}, error={}", jobId, e.getMessage(), e);
            logger.error("========================================");

            job.setStatus("FAILED");
            job.setCompletedAt(new Timestamp(System.currentTimeMillis()));
            job.setErrorMessage(e.getMessage());
            syncJobRepository.save(job);
        }
    }

    /**
     * Update job progress with estimated time remaining
     */
    private void updateJobProgress(ElasticsearchSyncJob job, long processed, long success, 
                                   long failure, int offset, long total, long startTime) {
        job.setProcessedRecords(processed);
        job.setSuccessCount(success);
        job.setFailureCount(failure);
        job.setCurrentOffset(offset);

        long elapsedTime = System.currentTimeMillis() - startTime;
        double speed = processed / (elapsedTime / 1000.0);
        job.setProcessingSpeed(speed);

        if (speed > 0) {
            long remaining = total - processed;
            long estimatedSeconds = (long) (remaining / speed);
            job.setEstimatedTimeRemaining(estimatedSeconds);
        }

        syncJobRepository.save(job);

        logger.info("Progress: {}/{} ({:.2f}%) - Speed: {:.2f} rec/sec - ETA: {} sec", 
            processed, total, (processed * 100.0) / total, speed, job.getEstimatedTimeRemaining());
    }

    /**
     * Bulk index documents
     */
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
                            .document(doc)
                        )
                    );
                }
            }

            BulkResponse result = esClient.bulk(br.build());
            int successCount = 0;

            if (result.errors()) {
                for (BulkResponseItem item : result.items()) {
                    if (item.error() == null) {
                        successCount++;
                    }
                }
            } else {
                successCount = documents.size();
            }

            return successCount;

        } catch (Exception e) {
            logger.error("Error in bulk indexing: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Convert DTO to Document
     */
    private BeneficiaryDocument convertToDocument(BeneficiariesDTO dto) {
        if (dto == null) {
            return null;
        }

        try {
            BeneficiaryDocument doc = new BeneficiaryDocument();

            if (dto.getBenRegId() != null) {
                BigInteger benRegId = (BigInteger) dto.getBenRegId();
                doc.setBenId(benRegId.toString());
                doc.setBenRegId(benRegId.longValue());
            } else if (dto.getBenId() != null) {
                doc.setBenId(dto.getBenId().toString());
                if (dto.getBenId() instanceof BigInteger) {
                    doc.setBenRegId(((BigInteger) dto.getBenId()).longValue());
                }
            } else {
                return null;
            }

            doc.setPhoneNum(dto.getPreferredPhoneNum());

            if (dto.getBeneficiaryDetails() != null) {
                BenDetailDTO benDetails = dto.getBeneficiaryDetails();
                doc.setFirstName(benDetails.getFirstName());
                doc.setLastName(benDetails.getLastName());
                doc.setAge(benDetails.getBeneficiaryAge());
                doc.setGender(benDetails.getGender());
            }

            return doc;

        } catch (Exception e) {
            logger.error("Error converting DTO: {}", e.getMessage());
            return null;
        }
    }
}