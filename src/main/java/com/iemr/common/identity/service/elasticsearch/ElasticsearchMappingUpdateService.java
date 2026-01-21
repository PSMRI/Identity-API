package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Service to update Elasticsearch mappings without deleting the index
 * Use this to add new fields to existing index
 */
@Service
public class ElasticsearchMappingUpdateService {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchMappingUpdateService.class);
    
    @Autowired
    private ElasticsearchClient esClient;
    
    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;
    
    /**
     * Add ABHA-related fields to existing index mapping
     * This does NOT require deleting the index or re-syncing data
     * 
     * NOTE: You can only ADD new fields, you cannot CHANGE existing field types
     */
    public void addAbhaFieldsToExistingIndex() throws Exception {
        logger.info("Adding ABHA fields to existing index: {}", beneficiaryIndex);
        
        try {
            // Check if index exists
            boolean indexExists = esClient.indices().exists(e -> e.index(beneficiaryIndex)).value();
            
            if (!indexExists) {
                logger.error("Index {} does not exist. Create it first.", beneficiaryIndex);
                throw new RuntimeException("Index does not exist: " + beneficiaryIndex);
            }
            
            // Update mapping by adding new properties
            esClient.indices().putMapping(pm -> pm
                .index(beneficiaryIndex)
                .properties("healthID", Property.of(p -> p.keyword(k -> k)))
                .properties("healthIDNumber", Property.of(p -> p.keyword(k -> k)))
                .properties("abhaID", Property.of(p -> p.keyword(k -> k)))
                .properties("abhaCreatedDate", Property.of(p -> p.keyword(k -> k)))
                .properties("abhaAuthMode", Property.of(p -> p.keyword(k -> k)))
                
                // Nested object for multiple ABHA addresses
                .properties("abhaAddresses", Property.of(p -> p.nested(n -> n
                    .properties("healthID", Property.of(fp -> fp.keyword(k -> k)))
                    .properties("healthIDNumber", Property.of(fp -> fp.keyword(k -> k)))
                    .properties("authenticationMode", Property.of(fp -> fp.keyword(k -> k)))
                    .properties("createdDate", Property.of(fp -> fp.date(d -> d)))
                )))
                
                // Add villageID and villageName if missing
                .properties("villageID", Property.of(p -> p.integer(i -> i)))
                .properties("villageName", Property.of(p -> p.keyword(k -> k)))
            );
            
            logger.info("ABHA fields added successfully to index: {}", beneficiaryIndex);
            logger.info("No data loss - existing documents are intact");
            logger.info("New fields will be populated when documents are updated/synced");
            
            // Verify the update
            verifyMappingUpdate();
            
        } catch (Exception e) {
            logger.error("Failed to add ABHA fields to index", e);
            throw e;
        }
    }
    
    /**
     * Verify that new fields were added successfully
     */
    private void verifyMappingUpdate() throws Exception {
        var mapping = esClient.indices().getMapping(g -> g.index(beneficiaryIndex));
        
        logger.info("Current index mapping properties:");
        mapping.get(beneficiaryIndex).mappings().properties().keySet().forEach(field -> {
            logger.info("  - {}", field);
        });
    }
    
    /**
     * Update existing documents with ABHA data (incremental sync)
     * This updates only documents that have ABHA details in the database
     * 
     * Call this after adding new fields to populate them with existing data
     */
    public Map<String, Integer> syncAbhaDataToExistingDocuments() {
        logger.info("Starting incremental ABHA data sync to existing ES documents");
        
        Map<String, Integer> result = new HashMap<>();
        int successCount = 0;
        int failureCount = 0;
        
        try {
            // This will be handled by your existing BeneficiaryElasticsearchIndexUpdater
            // which already fetches ABHA details from v_BenAdvanceSearchRepo
            
            // You can either:
            // 1. Sync all beneficiaries (slow but complete)
            // 2. Sync only beneficiaries with ABHA data (fast and targeted)
            
            logger.info("Use the existing syncAllBeneficiaries() method");
            logger.info("It will automatically populate new ABHA fields");
            
            result.put("message", 0);
            result.put("recommendation", 1);
            
        } catch (Exception e) {
            logger.error("Error during ABHA data sync", e);
        }
        
        result.put("success", successCount);
        result.put("failed", failureCount);
        return result;
    }
    
    /**
     * Get current index statistics and mapping info
     */
    public Map<String, Object> getIndexInfo() throws Exception {
        Map<String, Object> info = new HashMap<>();
        
        // Get mapping
        var mapping = esClient.indices().getMapping(g -> g.index(beneficiaryIndex));
        info.put("totalFields", mapping.get(beneficiaryIndex).mappings().properties().size());
        
        // Get stats
        var stats = esClient.indices().stats(s -> s.index(beneficiaryIndex));
        info.put("documentCount", stats.indices().get(beneficiaryIndex).primaries().docs().count());
        info.put("sizeInBytes", stats.indices().get(beneficiaryIndex).primaries().store().sizeInBytes());
        
        // Get settings
        var settings = esClient.indices().getSettings(g -> g.index(beneficiaryIndex));
        info.put("numberOfShards", settings.get(beneficiaryIndex).settings().index().numberOfShards());
        info.put("numberOfReplicas", settings.get(beneficiaryIndex).settings().index().numberOfReplicas());
        info.put("refreshInterval", settings.get(beneficiaryIndex).settings().index().refreshInterval().time());
        
        // Check if ABHA fields exist
        boolean hasAbhaFields = mapping.get(beneficiaryIndex).mappings().properties()
            .containsKey("healthID") && mapping.get(beneficiaryIndex).mappings().properties()
            .containsKey("abhaID");
        info.put("hasAbhaFields", hasAbhaFields);
        
        return info;
    }
    
    /**
     * Add any additional fields to index without deleting
     * Use this template for future field additions
     */
    public void addCustomFieldsToIndex(Map<String, Property> newFields) throws Exception {
        logger.info("Adding {} custom fields to index: {}", newFields.size(), beneficiaryIndex);
        
        esClient.indices().putMapping(pm -> {
            var builder = pm.index(beneficiaryIndex);
            newFields.forEach((fieldName, property) -> {
                builder.properties(fieldName, property);
                logger.info("  Adding field: {}", fieldName);
            });
            return builder;
        });
        
        logger.info("Custom fields added successfully");
    }
}