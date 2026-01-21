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
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;

/**
 * Service to synchronize beneficiary data from database to Elasticsearch
 */
@Service
public class ElasticsearchSyncService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSyncService.class);

    // OPTIMIZED BATCH SIZES for maximum performance
    private static final int DB_FETCH_SIZE = 5000; // Fetch 5000 IDs at once
    private static final int ES_BULK_SIZE = 2000; // Index 2000 documents per bulk request

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private BeneficiaryTransactionHelper transactionalWrapper;

    @Autowired
    private BeneficiaryDocumentDataService documentDataService; // KEY: Batch service with ABHA

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    /**
     * Sync all beneficiaries using BATCH queries WITH ABHA
     * This replaces individual queries with batch fetching (50-100x faster)
     */
    public SyncResult syncAllBeneficiaries() {
        logger.info("STARTING OPTIMIZED BATCH SYNC WITH ABHA");

        SyncResult result = new SyncResult();
        long startTime = System.currentTimeMillis();

        try {
            // Get total count
            long totalCount = transactionalWrapper.countActiveBeneficiaries();
            logger.info("Total beneficiaries to sync: {}", totalCount);

            if (totalCount == 0) {
                logger.warn("No beneficiaries found to sync!");
                return result;
            }

            AtomicInteger processedCount = new AtomicInteger(0);
            AtomicInteger abhaEnrichedCount = new AtomicInteger(0);
            int offset = 0;
            List<BeneficiaryDocument> esBatch = new ArrayList<>(ES_BULK_SIZE);

            // Process in large chunks for maximum speed
            while (offset < totalCount) {
                long chunkStart = System.currentTimeMillis();

                // STEP 1: Fetch IDs in batch
                List<Object[]> batchIds = fetchBatchWithRetry(offset, DB_FETCH_SIZE);

                if (batchIds == null || batchIds.isEmpty()) {
                    logger.info("No more records to process.");
                    break;
                }

                logger.info("Fetched {} IDs at offset {}", batchIds.size(), offset);

                // STEP 2: Convert IDs to BigInteger list
                List<BigInteger> benRegIds = new ArrayList<>(batchIds.size());
                for (Object[] idRow : batchIds) {
                    BigInteger benRegId = convertToBigInteger(idRow[0]);
                    if (benRegId != null) {
                        benRegIds.add(benRegId);
                    }
                }

                // STEP 3: BATCH FETCH complete data WITH ABHA (CRITICAL OPTIMIZATION)
                // This single call replaces thousands of individual database queries
                logger.info("Batch fetching complete data with ABHA for {} beneficiaries...", benRegIds.size());
                List<BeneficiaryDocument> documents = documentDataService.getBeneficiariesBatch(benRegIds);
                logger.info("Retrieved {} complete documents", documents.size());

                // STEP 4: Count ABHA enriched documents and add to ES batch
                for (BeneficiaryDocument doc : documents) {
                    if (doc != null && doc.getBenId() != null) {

                        // Track ABHA enrichment
                        if (doc.getHealthID() != null || doc.getAbhaID() != null) {
                            abhaEnrichedCount.incrementAndGet();
                            logger.debug("Document {} has ABHA: healthID={}, abhaID={}",
                                    doc.getBenId(), doc.getHealthID(), doc.getAbhaID());
                        }

                        esBatch.add(doc);

                        // Bulk index when batch is full
                        if (esBatch.size() >= ES_BULK_SIZE) {
                            int indexed = bulkIndexDocuments(esBatch);
                            result.addSuccess(indexed);
                            result.addFailure(esBatch.size() - indexed);

                            int current = processedCount.addAndGet(esBatch.size());
                            logProgress(current, totalCount, abhaEnrichedCount.get(), startTime);

                            esBatch.clear();
                        }
                    } else {
                        result.addFailure();
                    }
                }

                long chunkTime = System.currentTimeMillis() - chunkStart;
                logger.info("Chunk processed in {}ms ({} docs/sec)",
                        chunkTime,
                        (batchIds.size() * 1000) / Math.max(chunkTime, 1));

                offset += DB_FETCH_SIZE;

                // Brief pause to prevent overwhelming the system
                Thread.sleep(50);
            }

            // Index remaining documents
            if (!esBatch.isEmpty()) {
                logger.info("Indexing final batch of {} documents...", esBatch.size());
                int indexed = bulkIndexDocuments(esBatch);
                result.addSuccess(indexed);
                result.addFailure(esBatch.size() - indexed);
                processedCount.addAndGet(esBatch.size());
            }

            long totalTime = System.currentTimeMillis() - startTime;
            double docsPerSecond = (processedCount.get() * 1000.0) / totalTime;

            logger.info("SYNC COMPLETED SUCCESSFULLY!");
            logger.info("Total Processed: {}", processedCount.get());
            logger.info("Successfully Indexed: {}", result.getSuccessCount());
            logger.info("Failed: {}", result.getFailureCount());
            logger.info("ABHA Enriched: {} ({} %)",
                    abhaEnrichedCount.get(),
                    String.format("%.2f", (abhaEnrichedCount.get() * 100.0) / processedCount.get()));
            logger.info("Total Time: {} seconds ({} minutes)", totalTime / 1000, totalTime / 60000);
            logger.info("Throughput: {:.2f} documents/second", docsPerSecond);

        } catch (Exception e) {
            logger.error("CRITICAL ERROR during sync: {}", e.getMessage(), e);
            result.setError(e.getMessage());
        }

        return result;
    }

    /**
     * Fetch batch with automatic retry on connection errors
     */
    private List<Object[]> fetchBatchWithRetry(int offset, int limit) {
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return transactionalWrapper.getBeneficiaryIdsBatch(offset, limit);
            } catch (Exception e) {
                logger.warn("Database fetch error (attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    throw new RuntimeException("Failed to fetch batch after " + maxRetries + " attempts", e);
                }
                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", ie);
                }
            }
        }
        return null;
    }

    /**
     * Convert various ID types to BigInteger
     */
    private BigInteger convertToBigInteger(Object idObj) {
        if (idObj instanceof BigInteger) {
            return (BigInteger) idObj;
        } else if (idObj instanceof Long) {
            return BigInteger.valueOf((Long) idObj);
        } else if (idObj instanceof Integer) {
            return BigInteger.valueOf((Integer) idObj);
        } else {
            logger.warn("Unsupported ID type: {}", idObj != null ? idObj.getClass() : "null");
            return null;
        }
    }

    /**
     * Progress logging with ETA and ABHA count
     */
    private void logProgress(int current, long total, int abhaCount, long startTime) {
        double progress = (current * 100.0) / total;
        long elapsed = System.currentTimeMillis() - startTime;
        long estimatedTotal = (long) (elapsed / (progress / 100.0));
        long remaining = estimatedTotal - elapsed;

        logger.info("Progress: {}/{} ({:.2f}%) | ABHA: {} | Elapsed: {}m | ETA: {}m | Speed: {:.0f} docs/sec",
                current, total, progress, abhaCount,
                elapsed / 60000,
                remaining / 60000,
                (current * 1000.0) / elapsed);
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
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        logger.error("Error indexing document {}: {}", item.id(), item.error().reason());
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
     * Sync a single beneficiary with ABHA
     */
    public boolean syncSingleBeneficiary(String benRegId) {
        try {
            BigInteger benRegIdBig = new BigInteger(benRegId);

            // Check existence
            boolean exists = transactionalWrapper.existsByBenRegId(benRegIdBig);
            if (!exists) {
                logger.error("Beneficiary does not exist in database: {}", benRegId);
                return false;
            }

            logger.info("Beneficiary exists in database. Fetching details with ABHA...");

            // Fetch document using batch service (includes ABHA)
            BeneficiaryDocument doc = documentDataService.getBeneficiaryFromDatabase(benRegIdBig);

            if (doc == null || doc.getBenId() == null) {
                logger.error("Failed to fetch beneficiary document");
                return false;
            }

            logger.info("Document created with ABHA. healthID={}, abhaID={}",
                    doc.getHealthID(), doc.getAbhaID());

            // Index to Elasticsearch
            esClient.index(i -> i
                    .index(beneficiaryIndex)
                    .id(doc.getBenId())
                    .document(doc).refresh(Refresh.WaitFor));

            logger.info("SUCCESS! Beneficiary {} synced to Elasticsearch with ABHA", benRegId);
            return true;

        } catch (Exception e) {
            logger.error("ERROR syncing beneficiary {}: {}", benRegId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Check sync status
     */
    public SyncStatus checkSyncStatus() {
        try {
            long dbCount = transactionalWrapper.countActiveBeneficiaries();
            long esCount = esClient.count(c -> c.index(beneficiaryIndex)).count();

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
            return "SyncResult{successCount=" + successCount + ", failureCount=" + failureCount +
                    ", error='" + error + "'}";
        }
    }

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
            return "SyncStatus{databaseCount=" + databaseCount + ", elasticsearchCount=" + elasticsearchCount +
                    ", synced=" + synced + ", missingCount=" + missingCount + ", error='" + error + "'}";
        }
    }
}