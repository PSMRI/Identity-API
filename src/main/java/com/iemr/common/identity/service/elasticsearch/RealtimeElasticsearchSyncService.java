package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;

/**
 * Service for real-time Elasticsearch synchronization
 * Triggers automatically when beneficiaries are created/updated in database
 */
@Service
public class RealtimeElasticsearchSyncService {

    private static final Logger logger = LoggerFactory.getLogger(RealtimeElasticsearchSyncService.class);

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private OptimizedBeneficiaryDataService dataService;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    @Value("${elasticsearch.enabled}")
    private boolean esEnabled;

    /**
     * Async method to sync a single beneficiary to Elasticsearch
     * Called after beneficiary is created/updated in database
     */
    @Async("elasticsearchSyncExecutor")
    public void syncBeneficiaryAsync(BigInteger benRegId) {
        if (!esEnabled) {
            logger.debug("Elasticsearch is disabled, skipping sync");
            return;
        }

        try {
            logger.info("Starting async sync for benRegId: {}", benRegId);

            // Fetch beneficiary from database - returns BeneficiaryDocument directly
            BeneficiaryDocument doc = dataService.getBeneficiaryFromDatabase(benRegId);

            if (doc == null) {
                logger.warn("Beneficiary not found in database: {}", benRegId);
                return;
            }

            if (doc.getBenId() == null) {
                logger.error("BeneficiaryDocument has null benId: {}", benRegId);
                return;
            }

            // Index to Elasticsearch
            IndexRequest<BeneficiaryDocument> request = IndexRequest.of(i -> i
                .index(beneficiaryIndex)
                .id(doc.getBenId())
                .document(doc)
            );

            esClient.index(request);

            logger.info("Successfully synced beneficiary to Elasticsearch: benRegId={}, benId={}", 
                benRegId, doc.getBenId());

        } catch (Exception e) {
            logger.error("Error syncing beneficiary {} to Elasticsearch: {}", benRegId, e.getMessage(), e);
        }
    }

    /**
     * Delete beneficiary from Elasticsearch
     */
    @Async("elasticsearchSyncExecutor")
    public void deleteBeneficiaryAsync(String benId) {
        if (!esEnabled) {
            logger.debug("Elasticsearch is disabled, skipping delete");
            return;
        }

        try {
            logger.info("Starting async delete for benId: {}", benId);

            DeleteRequest request = DeleteRequest.of(d -> d
                .index(beneficiaryIndex)
                .id(benId)
            );

            esClient.delete(request);

            logger.info("Successfully deleted beneficiary from Elasticsearch: benId={}", benId);

        } catch (Exception e) {
            logger.error("Error deleting beneficiary {} from Elasticsearch: {}", benId, e.getMessage(), e);
        }
    }
}