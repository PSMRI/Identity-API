package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
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
        logger.info("Creating index optimized for bulk indexing: {}", beneficiaryIndex);
        
        // Delete existing index if it exists
        if (esClient.indices().exists(e -> e.index(beneficiaryIndex)).value()) {
            logger.warn("Index {} already exists, deleting...", beneficiaryIndex);
            esClient.indices().delete(d -> d.index(beneficiaryIndex));
        }
        
        IndexSettings settings = IndexSettings.of(s -> s
            .refreshInterval(t -> t.time("-1"))  // -1 = disable refresh completely
            
            // Use 1 shard for datasets < 50GB (yours is ~784K records)
            .numberOfShards("1")  
            
            // No replicas during initial indexing 
            .numberOfReplicas("0")
            
            // Disable query cache during indexing
            .queries(q -> q
                .cache(c -> c.enabled(false))
            )
            
            .maxResultWindow(10000)
            
            .translog(t -> t
                .durability(TranslogDurability.Async)
                .syncInterval(ts -> ts.time("120s"))  // Longer interval for bulk ops
                .flushThresholdSize(fb -> fb.bytes("1gb"))  // Larger flush threshold
            )
            
            // Disable merge throttling during bulk indexing
            .merge(m -> m
                .scheduler(ms -> ms
                    .maxThreadCount(1)
                    .maxMergeCount(6)
                )
            )
            
            // Optimize for write performance
            .indexing(i -> i
                .slowlog(sl -> sl
                    .threshold(t -> t
                        .index(idx -> idx
                            .warn(w -> w.time("10s"))
                            .info(inf -> inf.time("5s"))
                        )
                    )
                )
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
                    .indexPrefixes(ip -> ip.minChars(2).maxChars(5))
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
            
            // Geographic fields
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
            
            // Permanent address fields
            .properties("permStateID", Property.of(p -> p.integer(i -> i)))
            .properties("permStateName", Property.of(p -> p.keyword(k -> k)))
            .properties("permDistrictID", Property.of(p -> p.integer(i -> i)))
            .properties("permDistrictName", Property.of(p -> p.keyword(k -> k)))
            .properties("permBlockID", Property.of(p -> p.integer(i -> i)))
            .properties("permBlockName", Property.of(p -> p.keyword(k -> k)))
            .properties("permVillageID", Property.of(p -> p.integer(i -> i)))
            .properties("permVillageName", Property.of(p -> p.keyword(k -> k)))
            
            // Identity fields
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
     Optimize for search after bulk indexing completes
     */
    public void optimizeForSearch() throws Exception {
        logger.info("Optimizing index for search performance...");
        
        // Step 1: Force refresh to make all documents searchable
        logger.info("Forcing refresh to make documents visible...");
        esClient.indices().refresh(r -> r.index(beneficiaryIndex));
        
        // Step 2: Update settings for search optimization
        logger.info("Updating index settings for production search...");
        esClient.indices().putSettings(s -> s
            .index(beneficiaryIndex)
            .settings(is -> is
                .refreshInterval(t -> t.time("1s"))  
                .numberOfReplicas("1")  
                .translog(t -> t
                    .durability(TranslogDurability.Request)  
                    .syncInterval(ts -> ts.time("5s"))
                )
                .queries(q -> q
                    .cache(c -> c.enabled(true))  
                )
            )
        );
        
        // Step 3: Force merge to optimize segment count
        logger.info("Force merging segments for optimal read performance...");
        esClient.indices().forcemerge(f -> f
            .index(beneficiaryIndex)
            .maxNumSegments(1L)  // Single segment per shard for best performance
            .flush(true)
        );
        
        logger.info("Index optimization completed");
    }
    
    /**
     * Full indexing workflow with progress tracking
     */
    public Map<String, Object> indexAllBeneficiaries() {
        logger.info("STARTING FULL BENEFICIARY INDEXING");
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Execute bulk indexing
            logger.info("PHASE 1: Bulk indexing beneficiaries...");
            ElasticsearchSyncService.SyncResult result = syncService.syncAllBeneficiaries();
            
            long indexingTime = System.currentTimeMillis() - startTime;
            logger.info("Bulk indexing completed in {} seconds", indexingTime / 1000);
            logger.info("Success: {}, Failed: {}", result.getSuccessCount(), result.getFailureCount());
            
            // Optimize for search
            logger.info("PHASE 2: Optimizing for search...");
            long optimizeStart = System.currentTimeMillis();
            optimizeForSearch();
            long optimizeTime = System.currentTimeMillis() - optimizeStart;
            logger.info("Optimization completed in {} seconds", optimizeTime / 1000);
            
            // Prepare response
            Map<String, Object> response = new HashMap<>();
            response.put("success", result.getSuccessCount());
            response.put("failed", result.getFailureCount());
            response.put("indexingTimeSeconds", indexingTime / 1000);
            response.put("optimizationTimeSeconds", optimizeTime / 1000);
            response.put("totalTimeSeconds", (System.currentTimeMillis() - startTime) / 1000);
            
            logger.info("INDEXING COMPLETE - Total time: {} seconds", 
                       (System.currentTimeMillis() - startTime) / 1000);
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error during indexing", e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", 0);
            response.put("failed", 0);
            response.put("error", e.getMessage());
            return response;
        }
    }
    
    /**
     * Check index health and statistics
     */
    public Map<String, Object> getIndexStats() throws Exception {
        var stats = esClient.indices().stats(s -> s.index(beneficiaryIndex));
        var settings = esClient.indices().getSettings(g -> g.index(beneficiaryIndex));
        
        Map<String, Object> info = new HashMap<>();
        info.put("documentCount", stats.indices().get(beneficiaryIndex).primaries().docs().count());
        info.put("sizeInBytes", stats.indices().get(beneficiaryIndex).primaries().store().sizeInBytes());
        info.put("refreshInterval", settings.get(beneficiaryIndex).settings().index().refreshInterval().time());
        info.put("numberOfShards", settings.get(beneficiaryIndex).settings().index().numberOfShards());
        info.put("numberOfReplicas", settings.get(beneficiaryIndex).settings().index().numberOfReplicas());
        
        return info;
    }
}