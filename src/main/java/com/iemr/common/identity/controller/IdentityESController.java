package com.iemr.common.identity.controller;


import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
public ResponseEntity<Map<String, Object>> advanceSearchBeneficiariesES(
        @RequestBody String searchFilter,
        HttpServletRequest request) {

    logger.info("IdentityESController.advanceSearchBeneficiariesES - start {}", searchFilter);
    Map<String, Object> response = new HashMap<>();

    try {
        JsonObject searchParams = JsonParser.parseString(searchFilter).getAsJsonObject();
        logger.info("Search params = {}", searchParams);

        String firstName = getString(searchParams, "firstName");
        String lastName = getString(searchParams, "lastName");
        Integer genderId = getInteger(searchParams, "genderId");
        Date dob = getDate(searchParams, "dob");
        String fatherName = getString(searchParams, "fatherName");
        String spouseName = getString(searchParams, "spouseName");
        String phoneNumber = getString(searchParams, "phoneNumber");
        String beneficiaryId = getString(searchParams, "beneficiaryId");
        String healthId = getString(searchParams, "healthId");
        String aadharNo = getString(searchParams, "aadharNo");
        Boolean is1097 = getBoolean(searchParams, "is1097");

        Integer stateId = getLocationInt(searchParams, "stateId");
        Integer districtId = getLocationInt(searchParams, "districtId");
        Integer blockId = getLocationInt(searchParams, "blockId");
        Integer villageId = getLocationInt(searchParams, "villageId");

        String jwtToken = CookieUtil.getJwtTokenFromCookie(request);
        Integer userID = Integer.parseInt(jwtUtil.getUserIdFromToken(jwtToken));

        logger.info(
            "ES Advance search - firstName={}, genderId={}, stateId={}, districtId={}, userId={}",
            firstName, genderId, stateId, districtId, userID
        );

        Map<String, Object> searchResults =
                idService.advancedSearchBeneficiariesES(
                        firstName, lastName, genderId, dob,
                        stateId, districtId, blockId, villageId,
                        fatherName, spouseName, phoneNumber,
                        beneficiaryId, healthId, aadharNo,
                        userID, null, is1097
                );

        response.put("data", searchResults.get("data"));
        response.put("count", searchResults.get("count"));
        response.put("source", searchResults.get("source"));
        response.put("statusCode", 200);
        response.put("errorMessage", "Success");
        response.put("status", "Success");

        return ResponseEntity.ok(response);

    } catch (Exception e) {
        logger.error("Error in beneficiary ES advance search", e);
        response.put("data", Collections.emptyList());
        response.put("count", 0);
        response.put("source", "error");
        response.put("statusCode", 500);
        response.put("errorMessage", e.getMessage());
        response.put("status", "Error");
        return ResponseEntity.status(500).body(response);
    }
}

// Helper methods to extract values from JsonObject

private String getString(JsonObject json, String key) {
    return json.has(key) && !json.get(key).isJsonNull()
            ? json.get(key).getAsString()
            : null;
}

private Integer getInteger(JsonObject json, String key) {
    return json.has(key) && !json.get(key).isJsonNull()
            ? json.get(key).getAsInt()
            : null;
}

private Boolean getBoolean(JsonObject json, String key) {
    return json.has(key) && !json.get(key).isJsonNull()
            ? json.get(key).getAsBoolean()
            : null;
}

private Date getDate(JsonObject json, String key) {
    if (json.has(key) && !json.get(key).isJsonNull()) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd")
                    .parse(json.get(key).getAsString());
        } catch (Exception e) {
            logger.error("Invalid date format for {} ", key, e);
        }
    }
    return null;
}

private Integer getLocationInt(JsonObject root, String field) {

    if (root.has(field) && !root.get(field).isJsonNull()) {
        return root.get(field).getAsInt();
    }

    String[] addressKeys = {
            "currentAddress",
            "permanentAddress",
            "emergencyAddress"
    };

    for (String addressKey : addressKeys) {
        if (root.has(addressKey) && root.get(addressKey).isJsonObject()) {
            JsonObject addr = root.getAsJsonObject(addressKey);
            if (addr.has(field) && !addr.get(field).isJsonNull()) {
                return addr.get(field).getAsInt();
            }
        }
    }
    return null;
}
    
}