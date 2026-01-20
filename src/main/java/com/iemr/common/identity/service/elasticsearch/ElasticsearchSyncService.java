package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.dto.AbhaAddressDTO;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.repo.BenMappingRepo;

/**
 * Service to synchronize beneficiary data from database to Elasticsearch
 */
@Service
public class ElasticsearchSyncService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSyncService.class);
    private static final int BATCH_SIZE = 100; // Reduced to 100 for better connection management
    private static final int ES_BULK_SIZE = 50; // Reduced to 50 for better performance
    private static final int PAUSE_AFTER_BATCHES = 5; // Pause after every 5 batches

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private BenMappingRepo mappingRepo;

    @Autowired
    private BeneficiaryDataService beneficiaryDataService;

    @Autowired
    private BeneficiaryTransactionHelper transactionalWrapper;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    /**
     * Sync all beneficiaries from database to Elasticsearch
     * This should be run as a one-time operation or scheduled job
     */
    public SyncResult syncAllBeneficiaries() {
        logger.info("Starting full beneficiary sync to Elasticsearch...");

        SyncResult result = new SyncResult();

        try {
            // Get total count using transactional wrapper
            long totalCount = transactionalWrapper.countActiveBeneficiaries();
            logger.info("Total beneficiaries to sync: {}", totalCount);

            if (totalCount == 0) {
                logger.warn("No beneficiaries found to sync!");
                return result;
            }

            AtomicInteger processedCount = new AtomicInteger(0);
            int offset = 0;
            int batchCounter = 0;
            List<BeneficiaryDocument> esBatch = new ArrayList<>();

            // Process in batches
            while (offset < totalCount) {
                logger.info("Fetching batch: offset={}, limit={}", offset, BATCH_SIZE);

                List<Object[]> batchIds = null;

                try {
                    // Use transactional wrapper to get fresh connection for each batch
                    batchIds = transactionalWrapper.getBeneficiaryIdsBatch(offset, BATCH_SIZE);
                } catch (Exception e) {
                    logger.error("Error fetching batch from database: {}", e.getMessage());
                    // Wait and retry once
                    try {
                        Thread.sleep(2000);
                        batchIds = transactionalWrapper.getBeneficiaryIdsBatch(offset, BATCH_SIZE);
                    } catch (Exception e2) {
                        logger.error("Retry failed: {}", e2.getMessage());
                        result.setError("Database connection error: " + e2.getMessage());
                        break;
                    }
                }

                if (batchIds == null || batchIds.isEmpty()) {
                    logger.info("No more records to process. Breaking loop.");
                    break;
                }

                logger.info("Processing {} beneficiaries in current batch", batchIds.size());

                for (Object[] benIdObj : batchIds) {
                    try {

                        Object idObj = benIdObj[0];
                        BigInteger benRegId;

                        if (idObj instanceof BigInteger) {
                            benRegId = (BigInteger) idObj;
                        } else if (idObj instanceof Long) {
                            benRegId = BigInteger.valueOf((Long) idObj);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unsupported benRegId type: " + idObj.getClass());
                        }

                        // Fetch beneficiary details DIRECTLY from database
                        BeneficiariesDTO benDTO = beneficiaryDataService.getBeneficiaryFromDatabase(benRegId);

                        if (benDTO != null) {
                            BeneficiaryDocument doc = convertToDocument(benDTO);

                            if (doc != null && doc.getBenId() != null) {
                                esBatch.add(doc);

                                // Send to ES when batch is full
                                if (esBatch.size() >= ES_BULK_SIZE) {
                                    int indexed = bulkIndexDocuments(esBatch);
                                    result.addSuccess(indexed);
                                    result.addFailure(esBatch.size() - indexed);

                                    int current = processedCount.addAndGet(esBatch.size());
                                    logger.info("Progress: {}/{} ({} %) - Indexed: {}, Failed: {}",
                                            current, totalCount,
                                            String.format("%.2f", (current * 100.0) / totalCount),
                                            indexed, esBatch.size() - indexed);

                                    esBatch.clear();
                                }
                            } else {
                                logger.warn("Skipping beneficiary with null benId: benRegId={}", benRegId);
                                result.addFailure();
                            }
                        } else {
                            logger.warn("No details found for benRegId: {}", benRegId);
                            result.addFailure();
                        }

                    } catch (Exception e) {
                        logger.error("Error processing beneficiary in batch: {}", e.getMessage(), e);
                        result.addFailure();
                    }
                }

                offset += BATCH_SIZE;
                batchCounter++;

                // Pause after every N batches to let connections stabilize
                if (batchCounter % PAUSE_AFTER_BATCHES == 0) {
                    logger.info("Completed {} batches. Pausing for 2 seconds...", batchCounter);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        logger.warn("Sleep interrupted: {}", e.getMessage());
                    }
                }
            }

            // Index remaining documents
            if (!esBatch.isEmpty()) {
                logger.info("Indexing final batch of {} documents...", esBatch.size());
                int indexed = bulkIndexDocuments(esBatch);
                result.addSuccess(indexed);
                result.addFailure(esBatch.size() - indexed);
                processedCount.addAndGet(esBatch.size());
            }

            logger.info("Sync completed successfully!");
            logger.info("Total Processed: {}", processedCount.get());
            logger.info("Successfully Indexed: {}", result.getSuccessCount());
            logger.info("Failed: {}", result.getFailureCount());

        } catch (Exception e) {
            logger.error("========================================");
            logger.error("CRITICAL ERROR during full sync: {}", e.getMessage(), e);
            logger.error("========================================");
            result.setError(e.getMessage());
        }

        return result;
    }

    /**
     * Sync a single beneficiary by BenRegId
     * Uses direct database access to avoid Elasticsearch circular dependency
     */
    public boolean syncSingleBeneficiary(String benRegId) {
        try {

            BigInteger benRegIdBig = new BigInteger(benRegId);

            // Check if beneficiary exists in database first using transactional wrapper
            boolean exists = transactionalWrapper.existsByBenRegId(benRegIdBig);
            if (!exists) {
                logger.error("Beneficiary does not exist in database}");
                return false;
            }

            logger.info("Beneficiary exists in database. Fetching details...");

            // Get beneficiary DIRECTLY from database (not through IdentityService)
            BeneficiariesDTO benDTO = beneficiaryDataService.getBeneficiaryFromDatabase(benRegIdBig);

            if (benDTO == null) {
                logger.error("Failed to fetch beneficiary details from database");
                return false;
            }

            logger.info("Beneficiary details fetched successfully");
            logger.info("BenRegId: {}, Name: {} {}",
                    benDTO.getBenRegId(),
                    benDTO.getBeneficiaryDetails() != null ? benDTO.getBeneficiaryDetails().getFirstName() : "N/A",
                    benDTO.getBeneficiaryDetails() != null ? benDTO.getBeneficiaryDetails().getLastName() : "N/A");

            // Convert to Elasticsearch document
            BeneficiaryDocument doc = convertToDocument(benDTO);

            if (doc == null || doc.getBenId() == null) {
                logger.error("Failed to convert beneficiary to document");
                return false;
            }

            logger.info("Document created. Indexing to Elasticsearch...");
            logger.info("Document ID: {}, Index: {}", doc.getBenId(), beneficiaryIndex);

            // Index to Elasticsearch
            esClient.index(i -> i
                    .index(beneficiaryIndex)
                    .id(doc.getBenId())
                    .document(doc));

            logger.info("SUCCESS! Beneficiary synced to Elasticsearch");

            return true;

        } catch (Exception e) {
            logger.error("ERROR syncing beneficiary {}: {}", benRegId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Bulk index documents to Elasticsearch
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
                                    .document(doc)));
                }
            }

            BulkResponse result = esClient.bulk(br.build());

            int successCount = 0;

            if (result.errors()) {
                logger.warn("Bulk indexing had some errors");
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        logger.error("Error indexing document {}: {}",
                                item.id(), item.error().reason());
                    } else {
                        successCount++;
                    }
                }
            } else {
                successCount = documents.size();
            }

            return successCount;

        } catch (Exception e) {
            logger.error("Critical error in bulk indexing: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Convert BeneficiariesDTO to BeneficiaryDocument
     */
    private BeneficiaryDocument convertToDocument(BeneficiariesDTO dto) {
        if (dto == null) {
            logger.warn("Cannot convert null DTO to document");
            return null;
        }

        try {
            BeneficiaryDocument doc = new BeneficiaryDocument();

            // BenId (use benRegId as primary identifier)
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
                logger.warn("Beneficiary has no valid ID!");
                return null;
            }

            // Phone number
            doc.setPhoneNum(dto.getPreferredPhoneNum());

            // Beneficiary Details (from nested DTO)
            if (dto.getBeneficiaryDetails() != null) {
                BenDetailDTO benDetails = dto.getBeneficiaryDetails();
                doc.setFirstName(benDetails.getFirstName());
                doc.setLastName(benDetails.getLastName());
                doc.setAge(benDetails.getBeneficiaryAge());
                doc.setGender(benDetails.getGender());
            }
            // ===== EXTRACT ABHA DETAILS =====
            if (dto.getAbhaDetails() != null && !dto.getAbhaDetails().isEmpty()) {
                AbhaAddressDTO abhaDTO = dto.getAbhaDetails().get(0);

                if (abhaDTO.getHealthID() != null) {
                    doc.setHealthID(abhaDTO.getHealthID());
                    logger.debug("Set healthID={} for benRegId={}", abhaDTO.getHealthID(), doc.getBenRegId());
                }

                if (abhaDTO.getHealthIDNumber() != null) {
                    doc.setAbhaID(abhaDTO.getHealthIDNumber());
                    logger.debug("Set abhaID={} for benRegId={}", abhaDTO.getHealthIDNumber(), doc.getBenRegId());
                }
            }
            logger.debug("Successfully converted DTO to document: benId={}", doc.getBenId());
            return doc;

        } catch (Exception e) {
            logger.error("Error converting DTO to document: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Verify sync by checking document count
     */
    public SyncStatus checkSyncStatus() {
        try {
            long dbCount = transactionalWrapper.countActiveBeneficiaries();

            long esCount = esClient.count(c -> c
                    .index(beneficiaryIndex)).count();

            SyncStatus status = new SyncStatus();
            status.setDatabaseCount(dbCount);
            status.setElasticsearchCount(esCount);
            status.setSynced(dbCount == esCount);
            status.setMissingCount(dbCount - esCount);

            logger.info("Sync Status - DB: {}, ES: {}, Missing: {}", dbCount, esCount, dbCount - esCount);

            return status;

        } catch (Exception e) {
            logger.error("Error checking sync status: {}", e.getMessage(), e);
            SyncStatus status = new SyncStatus();
            status.setError(e.getMessage());
            return status;
        }
    }

    /**
     * Result class to track sync progress
     */
    public static class SyncResult {
        private int successCount = 0;
        private int failureCount = 0;
        private String error;

        public void addSuccess(int count) {
            this.successCount += count;
        }

        public void addFailure() {
            this.failureCount++;
        }

        public void addFailure(int count) {
            this.failureCount += count;
        }

        public int getSuccessCount() {
            return successCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return "SyncResult{" +
                    "successCount=" + successCount +
                    ", failureCount=" + failureCount +
                    ", error='" + error + '\'' +
                    '}';
        }
    }

    /**
     * Status class to track sync verification
     */
    public static class SyncStatus {
        private long databaseCount;
        private long elasticsearchCount;
        private boolean synced;
        private long missingCount;
        private String error;

        public long getDatabaseCount() {
            return databaseCount;
        }

        public void setDatabaseCount(long databaseCount) {
            this.databaseCount = databaseCount;
        }

        public long getElasticsearchCount() {
            return elasticsearchCount;
        }

        public void setElasticsearchCount(long elasticsearchCount) {
            this.elasticsearchCount = elasticsearchCount;
        }

        public boolean isSynced() {
            return synced;
        }

        public void setSynced(boolean synced) {
            this.synced = synced;
        }

        public long getMissingCount() {
            return missingCount;
        }

        public void setMissingCount(long missingCount) {
            this.missingCount = missingCount;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return "SyncStatus{" +
                    "databaseCount=" + databaseCount +
                    ", elasticsearchCount=" + elasticsearchCount +
                    ", synced=" + synced +
                    ", missingCount=" + missingCount +
                    ", error='" + error + '\'' +
                    '}';
        }
    }
}