package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ElasticsearchIndexingService {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchIndexingService.class);
    
    @Autowired
    private ElasticsearchClient esClient;
    
    @Autowired
    private ElasticsearchSyncService syncService;
    
    @Value("${elasticsearch.index.beneficiary:beneficiary_index_v5}")
    private String beneficiaryIndex;
    
    /**
     * Create or recreate the Elasticsearch index with proper mapping
     */
    public void createIndexWithMapping() throws Exception {
        logger.info("Creating index with mapping: {}", beneficiaryIndex);
        
        // Delete existing index if it exists
        if (esClient.indices().exists(e -> e.index(beneficiaryIndex)).value()) {
            logger.warn("Index {} already exists, deleting...", beneficiaryIndex);
            esClient.indices().delete(d -> d.index(beneficiaryIndex));
        }
        
        // Create index with mapping
        TypeMapping mapping = TypeMapping.of(tm -> tm
            .properties("benId", Property.of(p -> p.keyword(k -> k)))
            .properties("benRegId", Property.of(p -> p.long_(l -> l)))
            .properties("beneficiaryID", Property.of(p -> p.keyword(k -> k)))
            .properties("firstName", Property.of(p -> p.text(t -> t
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k))))))
            .properties("lastName", Property.of(p -> p.text(t -> t
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k))))))
            .properties("genderID", Property.of(p -> p.integer(i -> i)))
            .properties("genderName", Property.of(p -> p.keyword(k -> k)))
            .properties("dOB", Property.of(p -> p.date(d -> d)))
            .properties("age", Property.of(p -> p.integer(i -> i)))
            .properties("phoneNum", Property.of(p -> p.keyword(k -> k)))
            .properties("fatherName", Property.of(p -> p.text(t -> t
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k))))))
            .properties("spouseName", Property.of(p -> p.text(t -> t
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k))))))
            .properties("isHIVPos", Property.of(p -> p.keyword(k -> k)))
            .properties("createdBy", Property.of(p -> p.keyword(k -> k)))
            .properties("createdDate", Property.of(p -> p.date(d -> d)))
            .properties("lastModDate", Property.of(p -> p.long_(l -> l)))
            .properties("benAccountID", Property.of(p -> p.long_(l -> l)))
            
            // Health IDs
            .properties("healthID", Property.of(p -> p.keyword(k -> k)))
            .properties("abhaID", Property.of(p -> p.keyword(k -> k)))
            .properties("familyID", Property.of(p -> p.keyword(k -> k)))
            
            // Current Address
            .properties("stateID", Property.of(p -> p.integer(i -> i)))
            .properties("stateName", Property.of(p -> p.keyword(k -> k)))
            .properties("districtID", Property.of(p -> p.integer(i -> i)))
            .properties("districtName", Property.of(p -> p.keyword(k -> k)))
            .properties("blockID", Property.of(p -> p.integer(i -> i)))
            .properties("blockName", Property.of(p -> p.keyword(k -> k)))
            .properties("villageID", Property.of(p -> p.integer(i -> i)))
            .properties("villageName", Property.of(p -> p.keyword(k -> k)))
            .properties("pinCode", Property.of(p -> p.keyword(k -> k)))
            .properties("servicePointID", Property.of(p -> p.integer(i -> i)))
            .properties("servicePointName", Property.of(p -> p.keyword(k -> k)))
            .properties("parkingPlaceID", Property.of(p -> p.integer(i -> i)))
            
            // Permanent Address
            .properties("permStateID", Property.of(p -> p.integer(i -> i)))
            .properties("permStateName", Property.of(p -> p.keyword(k -> k)))
            .properties("permDistrictID", Property.of(p -> p.integer(i -> i)))
            .properties("permDistrictName", Property.of(p -> p.keyword(k -> k)))
            .properties("permBlockID", Property.of(p -> p.integer(i -> i)))
            .properties("permBlockName", Property.of(p -> p.keyword(k -> k)))
            .properties("permVillageID", Property.of(p -> p.integer(i -> i)))
            .properties("permVillageName", Property.of(p -> p.keyword(k -> k)))
            
            // Identity
            .properties("aadharNo", Property.of(p -> p.keyword(k -> k)))
            .properties("govtIdentityNo", Property.of(p -> p.keyword(k -> k)))
        );
        
        esClient.indices().create(c -> c
            .index(beneficiaryIndex)
            .mappings(mapping)
        );
        
        logger.info("Index created successfully: {}", beneficiaryIndex);
    }
    
    /**
     * Index all beneficiaries - delegates to existing sync service
     * This is much safer than loading all records at once
     */
    public Map<String, Integer> indexAllBeneficiaries() {
        logger.info("Starting full indexing via sync service...");
        
        try {
            // Use the existing, battle-tested sync service
            ElasticsearchSyncService.SyncResult result = syncService.syncAllBeneficiaries();
            
            Map<String, Integer> response = new HashMap<>();
            response.put("success", result.getSuccessCount());
            response.put("failed", result.getFailureCount());
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error during indexing", e);
            Map<String, Integer> response = new HashMap<>();
            response.put("success", 0);
            response.put("failed", 0);
            return response;
        }
    }
}