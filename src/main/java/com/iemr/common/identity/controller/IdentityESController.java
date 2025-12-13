package com.iemr.common.identity.controller;


import java.util.HashMap;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchService;
import com.iemr.common.identity.utils.CookieUtil;
import com.iemr.common.identity.utils.JwtUtil;

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

    /**
     * MAIN UNIVERSAL SEARCH ENDPOINT
     * Searches across all fields - name, phone, ID, etc.
     * 
     * Usage: GET /beneficiary/search?q=vani
     * Usage: GET /beneficiary/search?q=9876543210
     * Usage: GET /beneficiary/search?q=rajesh kumar
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
    
    
}