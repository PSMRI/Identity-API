package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.TranslogDurability;

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
    
    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;
    
    /**
     
     */
    public void createIndexWithMapping() throws Exception {
        logger.info("Creating index with mapping: {}", beneficiaryIndex);
        
        // Delete existing index if it exists
        if (esClient.indices().exists(e -> e.index(beneficiaryIndex)).value()) {
            logger.warn("Index {} already exists, deleting...", beneficiaryIndex);
            esClient.indices().delete(d -> d.index(beneficiaryIndex));
        }
        
        IndexSettings settings = IndexSettings.of(s -> s
            .refreshInterval(t -> t.time("30s"))
            
            .numberOfShards("3")  
            
            .numberOfReplicas("1")
            
            .queries(q -> q
                .cache(c -> c.enabled(true))
            )
            
            .maxResultWindow(10000)
            
            .translog(t -> t
                 .durability(TranslogDurability.Async)
                .syncInterval(ts -> ts.time("30s"))
            )
        );
        
        TypeMapping mapping = TypeMapping.of(tm -> tm
            .properties("benId", Property.of(p -> p.keyword(k -> k)))
            .properties("benRegId", Property.of(p -> p.long_(l -> l)))
            
            .properties("beneficiaryID", Property.of(p -> p.keyword(k -> k)))
            
            .properties("firstName", Property.of(p -> p.text(t -> t
                .analyzer("standard")  
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k.ignoreAbove(256))))
                .fields("prefix", Property.of(fp -> fp.text(txt -> txt
                    .analyzer("standard")
                    .indexPrefixes(ip -> ip.minChars(2).maxChars(5))  // Fast prefix search
                )))
            )))
            
            .properties("lastName", Property.of(p -> p.text(t -> t
                .analyzer("standard")
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k.ignoreAbove(256))))
                .fields("prefix", Property.of(fp -> fp.text(txt -> txt
                    .analyzer("standard")
                    .indexPrefixes(ip -> ip.minChars(2).maxChars(5))
                )))
            )))
            
            .properties("fatherName", Property.of(p -> p.text(t -> t
                .analyzer("standard")
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k.ignoreAbove(256))))
            )))
            
            .properties("spouseName", Property.of(p -> p.text(t -> t
                .analyzer("standard")
                .fields("keyword", Property.of(fp -> fp.keyword(k -> k.ignoreAbove(256))))
            )))
            
            .properties("genderID", Property.of(p -> p.integer(i -> i)))
            .properties("genderName", Property.of(p -> p.keyword(k -> k)))
            .properties("dOB", Property.of(p -> p.date(d -> d.format("strict_date_optional_time||epoch_millis"))))
            .properties("age", Property.of(p -> p.integer(i -> i)))
            
            .properties("phoneNum", Property.of(p -> p.keyword(k -> k
                .fields("ngram", Property.of(fp -> fp.text(txt -> txt
                    .analyzer("standard")
                    .searchAnalyzer("standard")
                )))
            )))
            
            .properties("isHIVPos", Property.of(p -> p.keyword(k -> k)))
            .properties("createdBy", Property.of(p -> p.keyword(k -> k)))
            .properties("createdDate", Property.of(p -> p.date(d -> d)))
            .properties("lastModDate", Property.of(p -> p.long_(l -> l)))
            .properties("benAccountID", Property.of(p -> p.long_(l -> l)))
            
            .properties("healthID", Property.of(p -> p.keyword(k -> k)))
            .properties("abhaID", Property.of(p -> p.keyword(k -> k)))
            .properties("familyID", Property.of(p -> p.keyword(k -> k)))
            
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
            
            .properties("permStateID", Property.of(p -> p.integer(i -> i)))
            .properties("permStateName", Property.of(p -> p.keyword(k -> k)))
            .properties("permDistrictID", Property.of(p -> p.integer(i -> i)))
            .properties("permDistrictName", Property.of(p -> p.keyword(k -> k)))
            .properties("permBlockID", Property.of(p -> p.integer(i -> i)))
            .properties("permBlockName", Property.of(p -> p.keyword(k -> k)))
            .properties("permVillageID", Property.of(p -> p.integer(i -> i)))
            .properties("permVillageName", Property.of(p -> p.keyword(k -> k)))
            
            .properties("aadharNo", Property.of(p -> p.keyword(k -> k)))
            .properties("govtIdentityNo", Property.of(p -> p.keyword(k -> k)))
        );
        
        esClient.indices().create(c -> c
            .index(beneficiaryIndex)
            .settings(settings)
            .mappings(mapping)
        );
        
        logger.info("Index created successfully: {}", beneficiaryIndex);
    }
    
    /**
     * Reset refresh interval after bulk indexing completes
     * Call this after syncAllBeneficiaries() finishes
     */
    public void optimizeForSearch() throws Exception {
        logger.info("Optimizing index for search performance...");
        
        esClient.indices().putSettings(s -> s
            .index(beneficiaryIndex)
            .settings(is -> is
                .refreshInterval(t -> t.time("1s"))  
                .translog(t -> t.durability(TranslogDurability.Request))  
            )
        );
        
        esClient.indices().forcemerge(f -> f
            .index(beneficiaryIndex)
            .maxNumSegments(1L)  // Optimal for read-heavy workloads
        );
        
    }
    
    /**
     * Index all beneficiaries - delegates to existing sync service
     */
    public Map<String, Integer> indexAllBeneficiaries() {
        logger.info("Starting full indexing via sync service...");
        
        try {
            ElasticsearchSyncService.SyncResult result = syncService.syncAllBeneficiaries();
            
            // After indexing completes, optimize for search
            optimizeForSearch();
            
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