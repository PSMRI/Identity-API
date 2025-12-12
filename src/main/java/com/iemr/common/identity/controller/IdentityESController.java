package com.iemr.common.identity.controller;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchService;
import com.iemr.common.identity.mapper.InputMapper;

/**
 * Elasticsearch-enabled Beneficiary Search Controller
 * All search endpoints with ES support
 */
@RestController
@RequestMapping("/beneficiary")
public class IdentityESController {

    private static final Logger logger = LoggerFactory.getLogger(IdentityESController.class);

    @Autowired
    private ElasticsearchService elasticsearchService;

    /**
     * MAIN UNIVERSAL SEARCH ENDPOINT
     * Searches across all fields - name, phone, ID, etc.
     * 
     * Usage: GET /beneficiary/search?q=vani
     * Usage: GET /beneficiary/search?q=9876543210
     * Usage: GET /beneficiary/search?q=rajesh kumar
     */
   @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> search(@RequestParam String query) {
        try {
            List<Map<String, Object>> results = elasticsearchService.universalSearch(query);
            
            Map<String, Object> response = new HashMap<>();
            response.put("data", results);
            response.put("statusCode", 200);
            response.put("errorMessage", "Success");
            response.put("status", "Success");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("data", new ArrayList<>());
            errorResponse.put("statusCode", 500);
            errorResponse.put("errorMessage", e.getMessage());
            errorResponse.put("status", "Error");
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
     
    // @GetMapping("/search")
    // public ResponseEntity<Map<String, Object>> universalSearch(@RequestParam String q) {
    //     logger.info("Universal search request: query={}", q);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         List<BeneficiariesDTO> results = elasticsearchService.universalSearch(q);
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
    //         response.put("query", q);
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Universal search error: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    /**
     * Search by BeneficiaryID (exact match)
     * 
     * Usage: GET /beneficiary/search/benId/123456
     */
    // @GetMapping("/search/benId/{beneficiaryId}")
    // public ResponseEntity<Map<String, Object>> searchByBenId(@PathVariable String beneficiaryId) {
    //     logger.info("Search by benId: {}", beneficiaryId);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         BigInteger benId = new BigInteger(beneficiaryId);
    //         List<BeneficiariesDTO> results = elasticsearchService.searchByBenId(benId);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (NumberFormatException e) {
    //         response.put("status", "error");
    //         response.put("message", "Invalid beneficiary ID format");
    //         return ResponseEntity.badRequest().body(response);
            
    //     } catch (Exception e) {
    //         logger.error("Error searching by benId: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Search by BeneficiaryRegID (exact match)
    //  * 
    //  * Usage: GET /beneficiary/search/benRegId/987654
    //  */
    // @GetMapping("/search/benRegId/{benRegId}")
    // public ResponseEntity<Map<String, Object>> searchByBenRegId(@PathVariable String benRegId) {
    //     logger.info("Search by benRegId: {}", benRegId);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         BigInteger benRegIdBig = new BigInteger(benRegId);
    //         List<BeneficiariesDTO> results = elasticsearchService.searchByBenRegId(benRegIdBig);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (NumberFormatException e) {
    //         response.put("status", "error");
    //         response.put("message", "Invalid beneficiary registration ID format");
    //         return ResponseEntity.badRequest().body(response);
            
    //     } catch (Exception e) {
    //         logger.error("Error searching by benRegId: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Search by Phone Number (supports partial match)
    //  * 
    //  * Usage: GET /beneficiary/search/phone/9876543210
    //  * Usage: GET /beneficiary/search/phone/987654
    //  */
    // @GetMapping("/search/phone/{phoneNumber}")
    // public ResponseEntity<Map<String, Object>> searchByPhoneNumber(@PathVariable String phoneNumber) {
    //     logger.info("Search by phone: {}", phoneNumber);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         List<BeneficiariesDTO> results = elasticsearchService.searchByPhoneNum(phoneNumber);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Error searching by phone: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Search by First Name (fuzzy matching - handles typos)
    //  * 
    //  * Usage: GET /beneficiary/search/firstName/vani
    //  * Usage: GET /beneficiary/search/firstName/rajesh
    //  */
    // @GetMapping("/search/firstName/{firstName}")
    // public ResponseEntity<Map<String, Object>> searchByFirstName(@PathVariable String firstName) {
    //     logger.info("Search by firstName: {}", firstName);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         List<BeneficiariesDTO> results = elasticsearchService.searchByFirstName(firstName);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
    //         response.put("fuzzyMatchEnabled", true);
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Error searching by firstName: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Search by Last Name (fuzzy matching)
    //  * 
    //  * Usage: GET /beneficiary/search/lastName/kumar
    //  */
    // @GetMapping("/search/lastName/{lastName}")
    // public ResponseEntity<Map<String, Object>> searchByLastName(@PathVariable String lastName) {
    //     logger.info("Search by lastName: {}", lastName);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         List<BeneficiariesDTO> results = elasticsearchService.searchByLastName(lastName);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
    //         response.put("fuzzyMatchEnabled", true);
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Error searching by lastName: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Advanced multi-field search with query parameters
    //  * 
    //  * Usage: GET /beneficiary/search/advanced?firstName=vani&phoneNum=9876
    //  * Usage: GET /beneficiary/search/advanced?firstName=rajesh&lastName=kumar&gender=Male
    //  */
    // @GetMapping("/search/advanced")
    // public ResponseEntity<Map<String, Object>> advancedSearchGet(
    //         @RequestParam(required = false) String firstName,
    //         @RequestParam(required = false) String lastName,
    //         @RequestParam(required = false) String phoneNum,
    //         @RequestParam(required = false) String gender,
    //         @RequestParam(required = false) Integer age) {
        
    //     logger.info("Advanced search: firstName={}, lastName={}, phoneNum={}, gender={}, age={}", 
    //         firstName, lastName, phoneNum, gender, age);
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         List<BeneficiariesDTO> results = elasticsearchService.advancedSearch(
    //             firstName, lastName, phoneNum, gender, age
    //         );
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
    //         response.put("searchCriteria", Map.of(
    //             "firstName", firstName != null ? firstName : "N/A",
    //             "lastName", lastName != null ? lastName : "N/A",
    //             "phoneNum", phoneNum != null ? phoneNum : "N/A",
    //             "gender", gender != null ? gender : "N/A",
    //             "age", age != null ? age : "N/A"
    //         ));
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Advanced search error: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }

    // /**
    //  * Advanced search with POST body (for complex queries)
    //  * 
    //  * Usage: POST /beneficiary/search/advanced
    //  * Body: IdentitySearchDTO JSON
    //  */
    // @PostMapping("/search/advanced")
    // public ResponseEntity<Map<String, Object>> advancedSearchPost(@RequestBody String searchRequest) {
    //     logger.info("Advanced POST search request received");
        
    //     Map<String, Object> response = new HashMap<>();
    //     long startTime = System.currentTimeMillis();
        
    //     try {
    //         IdentitySearchDTO searchDTO = InputMapper.getInstance().gson()
    //             .fromJson(searchRequest, IdentitySearchDTO.class);
            
    //         List<BeneficiariesDTO> results = elasticsearchService.flexibleSearch(searchDTO);
            
    //         long timeTaken = System.currentTimeMillis() - startTime;
            
    //         response.put("status", "success");
    //         response.put("count", results.size());
    //         response.put("data", results);
    //         response.put("searchTime", timeTaken + "ms");
    //         response.put("elasticsearchEnabled", true);
            
    //         return ResponseEntity.ok(response);
            
    //     } catch (Exception e) {
    //         logger.error("Advanced POST search error: {}", e.getMessage(), e);
    //         response.put("status", "error");
    //         response.put("message", e.getMessage());
    //         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    //     }
    // }
}