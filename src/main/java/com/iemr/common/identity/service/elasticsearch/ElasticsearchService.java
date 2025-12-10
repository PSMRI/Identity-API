package com.iemr.common.identity.service.elasticsearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.WildcardQuery;
import co.elastic.clients.elasticsearch._types.FieldValue;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.dto.BenDetailDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;

/**
 * Service for Elasticsearch search operations
 * Provides fast search functionality for beneficiaries
 */
@Service
public class ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    @Autowired
    private ElasticsearchClient esClient;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    @Value("${elasticsearch.enabled:false}")
    private boolean esEnabled;

    /**
     * Search beneficiary by BeneficiaryID (exact match)
     */
    public List<BeneficiariesDTO> searchByBenId(BigInteger benId) {
        if (!esEnabled || benId == null) {
            return new ArrayList<>();
        }

        try {
            logger.info("Searching Elasticsearch by benId: {}", benId);

            Query query = TermQuery.of(t -> t
                .field("benId.keyword")
                .value(FieldValue.of(benId.toString()))
            )._toQuery();

            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error searching by benId: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Search beneficiary by BeneficiaryRegID (exact match)
     */
    public List<BeneficiariesDTO> searchByBenRegId(BigInteger benRegId) {
        if (!esEnabled || benRegId == null) {
            return new ArrayList<>();
        }

        try {
            logger.info("Searching Elasticsearch by benRegId: {}", benRegId);

            Query query = TermQuery.of(t -> t
                .field("benRegId")
                .value(FieldValue.of(benRegId.longValue()))
            )._toQuery();

            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error searching by benRegId: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Search beneficiary by Phone Number (supports partial match)
     */
    public List<BeneficiariesDTO> searchByPhoneNum(String phoneNum) {
        if (!esEnabled || phoneNum == null || phoneNum.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try {
            logger.info("Searching Elasticsearch by phoneNum: {}", phoneNum);

            // Clean phone number (remove +91, spaces, etc.)
            String cleanedPhone = cleanPhoneNumber(phoneNum);

            Query query = WildcardQuery.of(w -> w
                .field("phoneNum.keyword")
                .value("*" + cleanedPhone + "*")
            )._toQuery();

            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error searching by phoneNum: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Search beneficiary by First Name (fuzzy match - handles typos)
     */
    public List<BeneficiariesDTO> searchByFirstName(String firstName) {
        if (!esEnabled || firstName == null || firstName.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try {
            logger.info("Searching Elasticsearch by firstName: {}", firstName);

            Query query = MatchQuery.of(m -> m
                .field("firstName")
                .query(firstName)
                .fuzziness("AUTO") // Tolerates 1-2 character differences
            )._toQuery();

            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error searching by firstName: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Search beneficiary by Last Name (fuzzy match)
     */
    public List<BeneficiariesDTO> searchByLastName(String lastName) {
        if (!esEnabled || lastName == null || lastName.trim().isEmpty()) {
            return new ArrayList<>();
        }

        try {
            logger.info("Searching Elasticsearch by lastName: {}", lastName);

            Query query = MatchQuery.of(m -> m
                .field("lastName")
                .query(lastName)
                .fuzziness("AUTO")
            )._toQuery();

            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error searching by lastName: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Advanced search with multiple criteria (AND condition)
     * Example: Search by firstName AND phoneNum
     */
    public List<BeneficiariesDTO> advancedSearch(String firstName, String lastName, 
                                                   String phoneNum, String gender, Integer age) {
        if (!esEnabled) {
            return new ArrayList<>();
        }

        try {
            logger.info("Advanced search: firstName={}, lastName={}, phoneNum={}, gender={}, age={}", 
                firstName, lastName, phoneNum, gender, age);

            BoolQuery.Builder boolQuery = new BoolQuery.Builder();

            // Add firstName filter (fuzzy)
            if (firstName != null && !firstName.trim().isEmpty()) {
                boolQuery.must(MatchQuery.of(m -> m
                    .field("firstName")
                    .query(firstName)
                    .fuzziness("AUTO")
                )._toQuery());
            }

            // Add lastName filter (fuzzy)
            if (lastName != null && !lastName.trim().isEmpty()) {
                boolQuery.must(MatchQuery.of(m -> m
                    .field("lastName")
                    .query(lastName)
                    .fuzziness("AUTO")
                )._toQuery());
            }

            // Add phoneNum filter (wildcard)
            if (phoneNum != null && !phoneNum.trim().isEmpty()) {
                String cleanedPhone = cleanPhoneNumber(phoneNum);
                boolQuery.must(WildcardQuery.of(w -> w
                    .field("phoneNum.keyword")
                    .value("*" + cleanedPhone + "*")
                )._toQuery());
            }

            // Add gender filter (exact)
            if (gender != null && !gender.trim().isEmpty()) {
                boolQuery.must(TermQuery.of(t -> t
                    .field("gender.keyword")
                    .value(FieldValue.of(gender))
                )._toQuery());
            }

            // Add age filter (exact)
            if (age != null) {
                boolQuery.must(TermQuery.of(t -> t
                    .field("age")
                    .value(FieldValue.of(age))
                )._toQuery());
            }

            Query query = boolQuery.build()._toQuery();
            return executeSearch(query);

        } catch (Exception e) {
            logger.error("Error in advanced search: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Execute search query and convert results to DTOs
     */
    private List<BeneficiariesDTO> executeSearch(Query query) {
        try {
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(beneficiaryIndex)
                .query(query)
                .size(100) // Limit to 100 results
            );

            SearchResponse<BeneficiaryDocument> response = esClient.search(
                searchRequest, 
                BeneficiaryDocument.class
            );

            List<BeneficiariesDTO> results = new ArrayList<>();
            
            for (Hit<BeneficiaryDocument> hit : response.hits().hits()) {
                BeneficiaryDocument doc = hit.source();
                if (doc != null) {
                    results.add(convertToDTO(doc));
                }
            }

            logger.info("Found {} results from Elasticsearch", results.size());
            return results;

        } catch (Exception e) {
            logger.error("Error executing search: {}", e.getMessage(), e);
            throw new RuntimeException("Elasticsearch search failed", e);
        }
    }

    /**
     * Convert BeneficiaryDocument to BeneficiariesDTO
     */
    private BeneficiariesDTO convertToDTO(BeneficiaryDocument doc) {
        BeneficiariesDTO dto = new BeneficiariesDTO();
        
        if (doc.getBenId() != null) {
            dto.setBenId(new BigInteger(doc.getBenId()));
        }
        
        if (doc.getBenRegId() != null) {
            dto.setBenRegId(BigInteger.valueOf(doc.getBenRegId()));
        }
        
        dto.setPreferredPhoneNum(doc.getPhoneNum());
        
        BenDetailDTO detailDTO = new BenDetailDTO();
        detailDTO.setFirstName(doc.getFirstName());
        detailDTO.setLastName(doc.getLastName());
        detailDTO.setBeneficiaryAge(doc.getAge());
        detailDTO.setGender(doc.getGender());
        
        dto.setBeneficiaryDetails(detailDTO);
        
        return dto;
    }

    /**
     * Clean phone number for searching
     */
    private String cleanPhoneNumber(String phoneNumber) {
        if (phoneNumber == null) {
            return "";
        }
        
        // Remove +91, spaces, dashes
        return phoneNumber.trim()
            .replaceAll("\\+91", "")
            .replaceAll("\\s+", "")
            .replaceAll("-", "");
    }
    
}