package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
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
     * Create index optimized for BULK INDEXING
     * Settings will be updated for search after sync completes
     */
    public void createIndexWithMapping() throws Exception {
        logger.info("Creating index optimized for bulk indexing: {}", beneficiaryIndex);
        
        // Delete existing index if it exists
        if (esClient.indices().exists(e -> e.index(beneficiaryIndex)).value()) {
            logger.warn("Index {} already exists, deleting...", beneficiaryIndex);
            esClient.indices().delete(d -> d.index(beneficiaryIndex));
        }
        
        // PHASE 1 SETTINGS: Optimized for BULK INDEXING (maximum write speed)
        IndexSettings settings = IndexSettings.of(s -> s
            // CRITICAL: Disable refresh during bulk indexing
            .refreshInterval(t -> t.time("-1"))  // -1 = disable completely for max speed
            
            // Use 1 shard for datasets < 50GB (optimal for 784K records)
            .numberOfShards("1")  
            
            // No replicas during initial indexing (add later for HA)
            .numberOfReplicas("0")
            
            // Disable query cache during indexing
            .queries(q -> q
                .cache(c -> c.enabled(false))
            )
            
            .maxResultWindow(10000)
            
            // CRITICAL: Async translog for maximum speed
            .translog(t -> t
                .durability(TranslogDurability.Async)
                .syncInterval(ts -> ts.time("120s"))  // Longer interval for bulk ops
            )
        );
        
        // Field mappings (supports fast search)
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
        
        logger.info("Index created with BULK INDEXING optimization");
        logger.info("Settings: refresh=disabled, replicas=0, async_translog, 1 shard");
    }
    
    /**
     * PHASE 2: Optimize for SEARCH after bulk indexing completes
     * Call this AFTER indexAllBeneficiaries() finishes
     */
    public void optimizeForSearch() throws Exception {
        logger.info("PHASE 2: Optimizing index for SEARCH performance");
        
        // Step 1: Force refresh to make all documents searchable
        logger.info("Step 1/3: Forcing refresh to make documents visible...");
        esClient.indices().refresh(r -> r.index(beneficiaryIndex));
        logger.info("Documents are now searchable");
        
        // Step 2: Update settings for production search
        logger.info("Step 2/3: Updating index settings for production...");
        esClient.indices().putSettings(s -> s
            .index(beneficiaryIndex)
            .settings(is -> is
                .refreshInterval(t -> t.time("1s"))  // Enable 1s refresh for near real-time search
                .numberOfReplicas("1")  // Add replica for high availability
                .translog(t -> t
                    .durability(TranslogDurability.Request)  // Synchronous for data safety
                    .syncInterval(ts -> ts.time("5s"))
                )
                .queries(q -> q
                    .cache(c -> c.enabled(true))  // Enable query cache for faster searches
                )
            )
        );
        logger.info("Settings applied: refresh=1s, replicas=1, query_cache=enabled");
        
        // Step 3: Force merge to optimize segments
        logger.info("Step 3/3: Force merging segments for optimal read performance...");
        logger.info("This may take 5-15 minutes depending on data size...");
        esClient.indices().forcemerge(f -> f
            .index(beneficiaryIndex)
            .maxNumSegments(1L)  // Single segment per shard = fastest searches
            .flush(true)
        );
        logger.info("Segments merged to 1 per shard");
        
        logger.info("INDEX OPTIMIZATION COMPLETE!");
        logger.info("Index is now ready for searches");
    }
    
    /**
     * COMPLETE WORKFLOW: Create index + Sync data + Optimize
     * This is your existing endpoint, now with automatic optimization
     */
    public Map<String, Integer> indexAllBeneficiaries() {
        logger.info("COMPLETE INDEXING WORKFLOW");
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Execute bulk indexing (now uses optimized batch queries)
            logger.info("PHASE 1: Bulk indexing beneficiaries with batch queries...");
            ElasticsearchSyncService.SyncResult result = syncService.syncAllBeneficiaries();
            
            long indexingTime = System.currentTimeMillis() - startTime;
            logger.info("Bulk indexing completed in {} seconds ({} minutes)", 
                       indexingTime / 1000, indexingTime / 60000);
            logger.info("Success: {}, Failed: {}", result.getSuccessCount(), result.getFailureCount());
            
            // Optimize for search
            logger.info("");
            logger.info("PHASE 2: Optimizing for search...");
            long optimizeStart = System.currentTimeMillis();
            optimizeForSearch();
            long optimizeTime = System.currentTimeMillis() - optimizeStart;
            logger.info("Optimization completed in {} seconds ({} minutes)", 
                       optimizeTime / 1000, optimizeTime / 60000);
            
            long totalTime = System.currentTimeMillis() - startTime;
            
            logger.info("COMPLETE WORKFLOW FINISHED!");
            logger.info("Total time: {} seconds ({} minutes)", totalTime / 1000, totalTime / 60000);
            logger.info("Indexing: {}m | Optimization: {}m", indexingTime / 60000, optimizeTime / 60000);
            
            // Return response in your existing format
            Map<String, Integer> response = new HashMap<>();
            response.put("success", result.getSuccessCount());
            response.put("failed", result.getFailureCount());
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error during indexing workflow", e);
            Map<String, Integer> response = new HashMap<>();
            response.put("success", 0);
            response.put("failed", 0);
            return response;
        }
    }
    
    /**
     * Get index statistics (unchanged)
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