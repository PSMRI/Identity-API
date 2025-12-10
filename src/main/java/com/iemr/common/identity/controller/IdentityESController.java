package com.iemr.common.identity.controller;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.service.IdentityService;
import com.iemr.common.identity.mapper.InputMapper;


/**
 * Enhanced Beneficiary Search Controller with Elasticsearch support
 * Provides fast search across multiple fields
 */
@RestController
@RequestMapping("/beneficiary")
public class IdentityESController {

    private static final Logger logger = LoggerFactory.getLogger(IdentityESController.class);

    @Autowired
    private IdentityService identityService;

    /**
     * Search beneficiary by BeneficiaryID
     * Uses Elasticsearch if enabled, falls back to MySQL
     * 
     * Usage: GET /beneficiary/search/benId/{beneficiaryId}
     * Example: GET /beneficiary/search/benId/123456
     */
    @GetMapping("/search/benId/{beneficiaryId}")
    public ResponseEntity<Map<String, Object>> searchByBenId(@PathVariable String beneficiaryId) {
        logger.info("Search request received: beneficiaryId={}", beneficiaryId);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            BigInteger benId = new BigInteger(beneficiaryId);
            List<BeneficiariesDTO> results = identityService.getBeneficiariesByBenId(benId);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (NumberFormatException e) {
            response.put("status", "error");
            response.put("message", "Invalid beneficiary ID format");
            return ResponseEntity.badRequest().body(response);
            
        } catch (Exception e) {
            logger.error("Error searching by beneficiaryId: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by BeneficiaryRegID
     * 
     * Usage: GET /beneficiary/search/benRegId/{benRegId}
     * Example: GET /beneficiary/search/benRegId/987654
     */
    @GetMapping("/search/benRegId/{benRegId}")
    public ResponseEntity<Map<String, Object>> searchByBenRegId(@PathVariable String benRegId) {
        logger.info("Search request received: benRegId={}", benRegId);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            BigInteger benRegIdBig = new BigInteger(benRegId);
            List<BeneficiariesDTO> results = identityService.getBeneficiariesByBenRegId(benRegIdBig);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (NumberFormatException e) {
            response.put("status", "error");
            response.put("message", "Invalid beneficiary registration ID format");
            return ResponseEntity.badRequest().body(response);
            
        } catch (Exception e) {
            logger.error("Error searching by benRegId: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by Phone Number
     * 
     * Usage: GET /beneficiary/search/phone/{phoneNumber}
     * Example: GET /beneficiary/search/phone/9876543210
     */
    @GetMapping("/search/phone/{phoneNumber}")
    public ResponseEntity<Map<String, Object>> searchByPhoneNumber(@PathVariable String phoneNumber) {
        logger.info("Search request received: phoneNumber={}", phoneNumber);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            List<BeneficiariesDTO> results = identityService.getBeneficiariesByPhoneNum(phoneNumber);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by phone: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by ABHA Address / HealthID
     * 
     * Usage: GET /beneficiary/search/healthId/{healthId}
     * Example: GET /beneficiary/search/healthId/rajesh@abdm
     */
    @GetMapping("/search/healthId/{healthId}")
    public ResponseEntity<Map<String, Object>> searchByHealthId(@PathVariable String healthId) {
        logger.info("Search request received: healthId={}", healthId);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            List<BeneficiariesDTO> results = identityService.getBeneficiaryByHealthIDAbhaAddress(healthId);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by healthId: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by ABHA ID Number / HealthIDNo
     * 
     * Usage: GET /beneficiary/search/healthIdNo/{healthIdNo}
     * Example: GET /beneficiary/search/healthIdNo/12345678901234
     */
    @GetMapping("/search/healthIdNo/{healthIdNo}")
    public ResponseEntity<Map<String, Object>> searchByHealthIdNo(@PathVariable String healthIdNo) {
        logger.info("Search request received: healthIdNo={}", healthIdNo);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            List<BeneficiariesDTO> results = identityService.getBeneficiaryByHealthIDNoAbhaIdNo(healthIdNo);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by healthIdNo: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by Family ID
     * 
     * Usage: GET /beneficiary/search/familyId/{familyId}
     * Example: GET /beneficiary/search/familyId/FAM12345
     */
    @GetMapping("/search/familyId/{familyId}")
    public ResponseEntity<Map<String, Object>> searchByFamilyId(@PathVariable String familyId) {
        logger.info("Search request received: familyId={}", familyId);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            List<BeneficiariesDTO> results = identityService.searhBeneficiaryByFamilyId(familyId);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by familyId: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by Government Identity (Aadhaar, etc.)
     * 
     * Usage: GET /beneficiary/search/govIdentity/{identityNo}
     * Example: GET /beneficiary/search/govIdentity/123456789012
     */
    @GetMapping("/search/govIdentity/{identityNo}")
    public ResponseEntity<Map<String, Object>> searchByGovIdentity(@PathVariable String identityNo) {
        logger.info("Search request received: govIdentity={}", identityNo);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            List<BeneficiariesDTO> results = identityService.searhBeneficiaryByGovIdentity(identityNo);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by govIdentity: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Advanced search with multiple criteria (with Elasticsearch support)
     * Supports fuzzy name matching via Elasticsearch
     * 
     * Usage: POST /beneficiary/search/advanced
     * Body: IdentitySearchDTO JSON
     */
    @PostMapping("/search/advanced")
    public ResponseEntity<Map<String, Object>> advancedSearch(@RequestBody String searchRequest) {
        logger.info("Advanced search request received");
    
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            IdentitySearchDTO searchDTO = InputMapper.getInstance().gson()
                .fromJson(searchRequest, IdentitySearchDTO.class);
            
            // Use the Elasticsearch-enabled method
            List<BeneficiariesDTO> results = identityService.getBeneficiarieswithES(searchDTO);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            response.put("elasticsearchEnabled", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error in advanced search: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Search beneficiary by Name (supports fuzzy matching via Elasticsearch)
     * 
     * Usage: GET /beneficiary/search/name?firstName=Rajesh&lastName=Kumar
     */
    @GetMapping("/search/name")
    public ResponseEntity<Map<String, Object>> searchByName(
            @RequestParam(required = false) String firstName,
            @RequestParam(required = false) String lastName) {
        
        logger.info("Name search request: firstName={}, lastName={}", firstName, lastName);
        
        Map<String, Object> response = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            if ((firstName == null || firstName.trim().isEmpty()) && 
                (lastName == null || lastName.trim().isEmpty())) {
                response.put("status", "error");
                response.put("message", "At least one of firstName or lastName is required");
                return ResponseEntity.badRequest().body(response);
            }
            
            // Create search DTO
            IdentitySearchDTO searchDTO = new IdentitySearchDTO();
            searchDTO.setFirstName(firstName);
            searchDTO.setLastName(lastName);
            
            // Use Elasticsearch-enabled search
            List<BeneficiariesDTO> results = identityService.getBeneficiarieswithES(searchDTO);
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("count", results.size());
            response.put("data", results);
            response.put("searchTime", timeTaken + "ms");
            response.put("fuzzySearchEnabled", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error searching by name: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Multi-field search endpoint
     * Searches across multiple fields simultaneously
     * 
     * Usage: GET /beneficiary/search/multi?query=rajesh&searchFields=name,phone
     */
    @GetMapping("/search/multi")
    public ResponseEntity<Map<String, Object>> multiFieldSearch(
            @RequestParam String query,
            @RequestParam(required = false, defaultValue = "name,phone,benId") String searchFields) {
        
        logger.info("Multi-field search: query={}, fields={}", query, searchFields);
        
        Map<String, Object> response = new HashMap<>();
        Map<String, List<BeneficiariesDTO>> resultsByField = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        try {
            String[] fields = searchFields.split(",");
            int totalResults = 0;
            
            for (String field : fields) {
                field = field.trim().toLowerCase();
                List<BeneficiariesDTO> fieldResults = null;
                
                try {
                    switch (field) {
                        case "name":
                            IdentitySearchDTO nameSearch = new IdentitySearchDTO();
                            nameSearch.setFirstName(query);
                            fieldResults = identityService.getBeneficiarieswithES(nameSearch);
                            break;
                            
                        case "phone":
                            fieldResults = identityService.getBeneficiariesByPhoneNum(query);
                            break;
                            
                        case "benid":
                            try {
                                BigInteger benId = new BigInteger(query);
                                fieldResults = identityService.getBeneficiariesByBenId(benId);
                            } catch (NumberFormatException e) {
                                // Skip if not a valid number
                            }
                            break;
                            
                        case "benregid":
                            try {
                                BigInteger benRegId = new BigInteger(query);
                                fieldResults = identityService.getBeneficiariesByBenRegId(benRegId);
                            } catch (NumberFormatException e) {
                                // Skip if not a valid number
                            }
                            break;
                    }
                    
                    if (fieldResults != null && !fieldResults.isEmpty()) {
                        resultsByField.put(field, fieldResults);
                        totalResults += fieldResults.size();
                    }
                    
                } catch (Exception e) {
                    logger.warn("Error searching field {}: {}", field, e.getMessage());
                }
            }
            
            long timeTaken = System.currentTimeMillis() - startTime;
            
            response.put("status", "success");
            response.put("totalResults", totalResults);
            response.put("resultsByField", resultsByField);
            response.put("searchTime", timeTaken + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error in multi-field search: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}