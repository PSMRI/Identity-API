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
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;

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

            // Fetch beneficiary from database
            BeneficiariesDTO benDTO = dataService.getBeneficiaryFromDatabase(benRegId);

            if (benDTO == null) {
                logger.warn("Beneficiary not found in database: {}", benRegId);
                return;
            }

            // Convert to Elasticsearch document
            BeneficiaryDocument doc = convertToDocument(benDTO);

            if (doc == null || doc.getBenId() == null) {
                logger.error("Failed to convert beneficiary to document: {}", benRegId);
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
     * Convert DTO to Document
     */
   private BeneficiaryDocument convertToDocument(BeneficiariesDTO dto) {
    if (dto == null) return null;

    try {
        BeneficiaryDocument doc = new BeneficiaryDocument();

        // IDs
        doc.setBenId(dto.getBenId() != null ? dto.getBenId().toString() : null);
        doc.setBenRegId(dto.getBenRegId() != null ? dto.getBenRegId().longValue() : null);

        // Phone
        if (dto.getContacts() != null && !dto.getContacts().isEmpty()) {
            doc.setPhoneNum(dto.getContacts().get(0).getPhoneNum());
        } else if (dto.getPreferredPhoneNum() != null) {
            doc.setPhoneNum(dto.getPreferredPhoneNum());
        }

        // Names
        if (dto.getBeneficiaryDetails() != null) {
            BenDetailDTO benDetails = dto.getBeneficiaryDetails();
            doc.setFirstName(benDetails.getFirstName());
            doc.setLastName(benDetails.getLastName());
            doc.setGender(benDetails.getGender());
        }

        // Age from DTO
        doc.setAge(dto.getBeneficiaryAge());

        // You can add district, village if available
        // doc.setDistrictName(...);
        // doc.setVillageName(...);

        return doc;

    } catch (Exception e) {
        logger.error("Error converting DTO to document: {}", e.getMessage());
        return null;
    }
}

}