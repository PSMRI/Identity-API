package com.iemr.common.identity.controller;


import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.mapper.InputMapper;
import com.iemr.common.identity.service.IdentityService;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchService;
import com.iemr.common.identity.utils.CookieUtil;
import com.iemr.common.identity.utils.JwtUtil;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.HttpServletRequest;

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

   	@Autowired
	private JwtUtil jwtUtil;

    @Autowired
    private IdentityService idService;
    /**
     * MAIN UNIVERSAL SEARCH ENDPOINT
     * Searches across all fields - name, phone, ID, etc.
     * 
     * Usage: GET /beneficiary/search?query=vani
     * Usage: GET /beneficiary/search?query=9876543210
     */
   @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> search(@RequestParam String query, HttpServletRequest request) {
        try {
            String jwtToken = CookieUtil.getJwtTokenFromCookie(request);
			String userId = jwtUtil.getUserIdFromToken(jwtToken);
            int userID=Integer.parseInt(userId);
            List<Map<String, Object>> results = elasticsearchService.universalSearch(query, userID);
            
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
    
    /**
 * NEW Elasticsearch-based advance search
 */
@Operation(summary = "Get beneficiaries by advance search using Elasticsearch")
@PostMapping(path = "/advanceSearchES", headers = "Authorization")
public ResponseEntity<Map<String, Object>> getBeneficiariesES(
        @RequestBody String searchFilter,
        HttpServletRequest request) {

    logger.info("IdentityESController.getBeneficiariesES - start");

    Map<String, Object> response = new HashMap<>();

    try {
        JsonElement json = JsonParser.parseString(searchFilter);
        IdentitySearchDTO searchParams =
                InputMapper.getInstance().gson().fromJson(json, IdentitySearchDTO.class);

        // Get userId from JWT token
        String jwtToken = CookieUtil.getJwtTokenFromCookie(request);
        String userId = jwtUtil.getUserIdFromToken(jwtToken);
        Integer userID = Integer.parseInt(userId);

        logger.info("ES Advance search for userId: {}", userID);

        // Call ES-enabled service
        List<BeneficiariesDTO> list = idService.getBeneficiarieswithES(searchParams);

        if (list != null) {
            list.removeIf(Objects::isNull);
            Collections.sort(list);
        }

        response.put("data", list != null ? list : new ArrayList<>());
        response.put("statusCode", 200);
        response.put("errorMessage", "Success");
        response.put("status", "Success");

        logger.info("IdentityESController.getBeneficiariesES - end. Found {} beneficiaries",
                list != null ? list.size() : 0);

        return ResponseEntity.ok(response);

    } catch (Exception e) {
        logger.error("Error in beneficiary ES advance search", e);

        response.put("data", new ArrayList<>());
        response.put("statusCode", 500);
        response.put("errorMessage", e.getMessage());
        response.put("status", "Error");

        return ResponseEntity.status(500).body(response);
    }
}


    
}