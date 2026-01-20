package com.iemr.common.identity.service.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScore;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.SearchResponse;

import com.iemr.common.identity.dto.BeneficiariesESDTO;
import com.iemr.common.identity.repo.BenDetailRepo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import co.elastic.clients.elasticsearch._types.SortOrder;
import com.iemr.common.identity.repo.BenAddressRepo;

@Service
public class ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private BenDetailRepo benDetailRepo;

    @Autowired
    private BenAddressRepo benAddressRepo;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    @Value("${elasticsearch.enabled}")
    private boolean esEnabled;

    /**
     * Universal search with score-based filtering and location ranking
     * Only returns records that actually match the query (not all 10000)
     */
    public List<Map<String, Object>> universalSearch(String query, Integer userId) {
        try {
            final Map<String, Integer> userLocation = (userId != null) ? getUserLocation(userId) : null;

            boolean isNumeric = query.matches("\\d+");
            double minScore = isNumeric ? 1.0 : 2.0;

            SearchResponse<BeneficiariesESDTO> response = esClient.search(s -> s
                    .index(beneficiaryIndex)

                    .preference("_local")

                    .requestCache(true)

                    .query(q -> q
                            .functionScore(fs -> fs
                                    .query(qq -> qq
                                            .bool(b -> {
                                                if (!isNumeric) {
                                                    // OPTIMIZED NAME SEARCH
                                                    // Use match_phrase_prefix for faster prefix matching
                                                    b.should(s1 -> s1.multiMatch(mm -> mm
                                                            .query(query)
                                                            .fields("firstName^3", "lastName^3")
                                                            .type(TextQueryType.Phrase)
                                                            .boost(3.0f)));

                                                    b.should(s2 -> s2.term(t -> t
                                                            .field("firstName.keyword")
                                                            .value(query)
                                                            .boost(10.0f)));
                                                    b.should(s3 -> s3.term(t -> t
                                                            .field("lastName.keyword")
                                                            .value(query)
                                                            .boost(10.0f)));

                                                    // Prefix search using index_prefixes (FAST!)
                                                    b.should(s4 -> s4.match(m -> m
                                                            .field("firstName.prefix")
                                                            .query(query)
                                                            .boost(2.0f)));
                                                    b.should(s5 -> s5.match(m -> m
                                                            .field("lastName.prefix")
                                                            .query(query)
                                                            .boost(2.0f)));
                                                }

                                                b.should(s6 -> s6
                                                        .term(t -> t.field("healthID").value(query).boost(15.0f)));
                                                b.should(s7 -> s7
                                                        .term(t -> t.field("abhaID").value(query).boost(15.0f)));
                                                b.should(s8 -> s8
                                                        .term(t -> t.field("beneficiaryID").value(query).boost(15.0f)));
                                                b.should(
                                                        s9 -> s9.term(t -> t.field("benId").value(query).boost(15.0f)));
                                                b.should(s10 -> s10
                                                        .term(t -> t.field("aadharNo").value(query).boost(12.0f)));

                                                if (isNumeric) {
                                                    // PREFIX QUERIES (much faster than wildcard)
                                                    b.should(s11 -> s11
                                                            .prefix(p -> p.field("phoneNum").value(query).boost(5.0f)));
                                                    b.should(s12 -> s12
                                                            .prefix(p -> p.field("healthID").value(query).boost(4.0f)));
                                                    b.should(s13 -> s13
                                                            .prefix(p -> p.field("abhaID").value(query).boost(4.0f)));
                                                    b.should(s14 -> s14.prefix(
                                                            p -> p.field("beneficiaryID").value(query).boost(4.0f)));

                                                    // ONLY use wildcard if query is long enough (>= 4 digits)
                                                    if (query.length() >= 4) {
                                                        b.should(s15 -> s15.wildcard(w -> w
                                                                .field("phoneNum")
                                                                .value("*" + query + "*")
                                                                .boost(2.0f)));
                                                    }

                                                    try {
                                                        Long numericValue = Long.parseLong(query);
                                                        b.should(s16 -> s16.term(t -> t.field("benRegId")
                                                                .value(numericValue).boost(15.0f)));
                                                        b.should(s17 -> s17.term(t -> t.field("benAccountID")
                                                                .value(numericValue).boost(10.0f)));

                                                        int intValue = numericValue.intValue();
                                                        if (userLocation != null) {
                                                            Integer userVillageId = userLocation.get("villageId");
                                                            Integer userBlockId = userLocation.get("blockId");

                                                            if (userVillageId != null && userVillageId == intValue) {
                                                                b.should(s18 -> s18.term(t -> t.field("villageID")
                                                                        .value(intValue).boost(3.0f)));
                                                            }
                                                            if (userBlockId != null && userBlockId == intValue) {
                                                                b.should(s19 -> s19.term(t -> t.field("blockID")
                                                                        .value(intValue).boost(2.0f)));
                                                            }
                                                        }
                                                    } catch (NumberFormatException e) {
                                                    }
                                                }

                                                b.minimumShouldMatch("1");
                                                return b;
                                            }))
                                    .functions(getFunctionScores(userLocation))
                                    .scoreMode(FunctionScoreMode.Sum)
                                    .boostMode(FunctionBoostMode.Multiply)
                                    .maxBoost(5.0)))
                    .minScore(minScore)

                    .size(100) // Reduced from 500

                    .sort(so -> so.score(sc -> sc.order(SortOrder.Desc)))

                    .source(src -> src
                            .filter(f -> f
                                    .includes("benRegId", "beneficiaryID", "firstName", "lastName",
                                            "genderID", "genderName", "dOB", "phoneNum",
                                            "stateID", "stateName", "districtID", "blockID", "villageID", "healthID", "abhaID",
                                            "abhaCreatedDate",
                                            "familyID",
                                            "fatherName", "spouseName", "age", "createdBy", "createdDate",
                                            "lastModDate", "benAccountID", "districtName", "blockName",
                                            "villageName", "pinCode", "servicePointID", "servicePointName",
                                            "parkingPlaceID", "permStateID", "permStateName", "permDistrictID",
                                            "permDistrictName", "permBlockID", "permBlockName", "permVillageID",
                                            "permVillageName")))

                    , BeneficiariesESDTO.class);

            logger.info("ES returned {} hits in {}ms for query: '{}'",
                    response.hits().hits().size(),
                    response.took(),
                    query);

            if (!response.hits().hits().isEmpty()) {
                BeneficiariesESDTO firstResult = response.hits().hits().get(0).source();
                logger.info("First result - benRegId: {}, healthID: {}, abhaID: {}",
                        firstResult.getBenRegId(),
                        firstResult.getHealthID(),
                        firstResult.getAbhaID());
            }

            if (response.hits().hits().isEmpty()) {
                logger.info("No results in ES, using database fallback");
                return searchInDatabaseDirectly(query);
            }

            List<Map<String, Object>> results = response.hits().hits().stream()
                    .map(hit -> {
                        Map<String, Object> result = mapESResultToExpectedFormat(hit.source());
                        if (result != null) {
                            result.put("_score", hit.score());
                        }
                        return result;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            logger.info("Returning {} results", results.size());
            return results;

        } catch (Exception e) {
            logger.error("ES search failed: {}", e.getMessage());
            return searchInDatabaseDirectly(query);
        }
    }

    /**
     * Generate function scores for location-based ranking
     */
    private List<FunctionScore> getFunctionScores(Map<String, Integer> userLocation) {
        if (userLocation == null) {
            return List.of();
        }

        List<FunctionScore> scores = new ArrayList<>();

        Integer userVillageId = userLocation.get("villageId");
        Integer userBlockId = userLocation.get("blockId");

        // Village match
        if (userVillageId != null) {
            scores.add(FunctionScore.of(f -> f
                    .filter(ff -> ff.term(t -> t.field("villageID").value(userVillageId)))
                    .weight(2.0)));
        }

        // Block match
        if (userBlockId != null) {
            scores.add(FunctionScore.of(f -> f
                    .filter(ff -> ff.term(t -> t.field("blockID").value(userBlockId)))
                    .weight(1.5)));
        }

        return scores;
    }

    /**
     * Advanced search with filter context
     */
    public List<Map<String, Object>> advancedSearch(
            String firstName,
            String lastName,
            Integer genderId,
            Date dob,
            Integer stateId,
            Integer districtId,
            Integer blockId,
            Integer villageId,
            String fatherName,
            String spouseName,
            String phoneNumber,
            String beneficiaryId,
            String healthId,
            String aadharNo,
            Integer userId) {

        try {
            final Map<String, Integer> userLocation = (userId != null) ? getUserLocation(userId) : null;

            SearchResponse<BeneficiariesESDTO> response = esClient.search(s -> s
                    .index(beneficiaryIndex)
                    .preference("_local")
                    .requestCache(true)

                    .query(q -> q
                            .bool(b -> {
                                // Use FILTER context for exact matches (faster, cached)
                                if (genderId != null) {
                                    b.filter(f -> f.term(t -> t.field("genderID").value(genderId)));
                                }
                                if (stateId != null) {
                                    b.filter(f -> f.term(t -> t.field("stateID").value(stateId)));
                                }
                                if (districtId != null) {
                                    b.filter(f -> f.term(t -> t.field("districtID").value(districtId)));
                                }
                                if (blockId != null) {
                                    b.filter(f -> f.term(t -> t.field("blockID").value(blockId)));
                                }
                                if (villageId != null) {
                                    b.filter(f -> f.term(t -> t.field("villageID").value(villageId)));
                                }

                                // MUST context for scored searches
                                if (firstName != null && !firstName.trim().isEmpty()) {
                                    b.must(m -> m.bool(bb -> bb
                                            .should(s1 -> s1.term(
                                                    t -> t.field("firstName.keyword").value(firstName).boost(5.0f)))
                                            .should(s2 -> s2
                                                    .match(mm -> mm.field("firstName").query(firstName).boost(2.0f)))
                                            .minimumShouldMatch("1")));
                                }

                                if (lastName != null && !lastName.trim().isEmpty()) {
                                    b.must(m -> m.bool(bb -> bb
                                            .should(s1 -> s1
                                                    .term(t -> t.field("lastName.keyword").value(lastName).boost(5.0f)))
                                            .should(s2 -> s2
                                                    .match(mm -> mm.field("lastName").query(lastName).boost(2.0f)))
                                            .minimumShouldMatch("1")));
                                }

                                // Exact match IDs in filter context
                                if (beneficiaryId != null && !beneficiaryId.trim().isEmpty()) {
                                    b.filter(f -> f.term(t -> t.field("beneficiaryID").value(beneficiaryId)));
                                }
                                if (healthId != null && !healthId.trim().isEmpty()) {
                                    b.filter(f -> f.bool(bb -> bb
                                            .should(s1 -> s1.term(t -> t.field("healthID").value(healthId)))
                                            .should(s2 -> s2.term(t -> t.field("abhaID").value(healthId)))
                                            .minimumShouldMatch("1")));
                                }
                                if (aadharNo != null && !aadharNo.trim().isEmpty()) {
                                    b.filter(f -> f.term(t -> t.field("aadharNo").value(aadharNo)));
                                }
                                if (phoneNumber != null && !phoneNumber.trim().isEmpty()) {
                                    b.must(m -> m.bool(bb -> bb
                                            .should(s1 -> s1
                                                    .term(t -> t.field("phoneNum").value(phoneNumber).boost(3.0f)))
                                            .should(s2 -> s2
                                                    .prefix(p -> p.field("phoneNum").value(phoneNumber).boost(2.0f)))
                                            .minimumShouldMatch("1")));
                                }

                                return b;
                            }))
                    .size(100)
                    .sort(so -> so.score(sc -> sc.order(SortOrder.Desc)))

                    , BeneficiariesESDTO.class);

            if (response.hits().hits().isEmpty()) {
                return searchInDatabaseForAdvanced(firstName, lastName, genderId, dob,
                        stateId, districtId, blockId, villageId, fatherName, spouseName,
                        phoneNumber, beneficiaryId, healthId, aadharNo);
            }

            return response.hits().hits().stream()
                    .map(hit -> mapESResultToExpectedFormat(hit.source()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("ES advanced search failed: {}", e.getMessage());
            return searchInDatabaseForAdvanced(firstName, lastName, genderId, dob,
                    stateId, districtId, blockId, villageId, fatherName, spouseName,
                    phoneNumber, beneficiaryId, healthId, aadharNo);
        }
    }

    /**
     * Database fallback for advanced search
     */
    private List<Map<String, Object>> searchInDatabaseForAdvanced(
            String firstName, String lastName, Integer genderId, Date dob,
            Integer stateId, Integer districtId, Integer blockId, Integer villageId,
            String fatherName, String spouseName, String phoneNumber,
            String beneficiaryId, String healthId, String aadharNo) {

        try {
            List<Object[]> results = benDetailRepo.advancedSearchBeneficiaries(
                    firstName, lastName, genderId, dob, stateId, districtId,
                    blockId, fatherName, spouseName, phoneNumber,
                    beneficiaryId);

            return results.stream()
                    .map(this::mapToExpectedFormat)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("Database advanced search failed: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * Overloaded method without userId (backward compatibility)
     */
    public List<Map<String, Object>> universalSearch(String query) {
        return universalSearch(query, null);
    }

    /**
     * Get user location from database
     */
    private Map<String, Integer> getUserLocation(Integer userId) {
        try {
            List<Object[]> results = benAddressRepo.getUserLocation(userId);
            if (results != null && !results.isEmpty()) {
                Object[] row = results.get(0);
                Map<String, Integer> location = new HashMap<>();
                location.put("psmId", getInteger(row[0]));
                location.put("blockId", getInteger(row[1]));
                location.put("villageId", getInteger(row[2]));
                location.put("servicePointId", getInteger(row[3]));
                return location;
            }
        } catch (Exception e) {
            logger.error("Error fetching user location: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * Rank results by location match priority
     * Priority: Village > Block > District > State > No Match
     */
    private List<Map<String, Object>> rankByLocation(List<Map<String, Object>> results,
            Map<String, Integer> userLocation) {
        Integer userBlockId = userLocation.get("blockId");
        Integer userVillageId = userLocation.get("villageId");

        logger.info("Ranking by location - User Block: {}, Village: {}", userBlockId, userVillageId);

        return results.stream()
                .sorted((r1, r2) -> {
                    int score1 = calculateLocationScore(r1, userBlockId, userVillageId);
                    int score2 = calculateLocationScore(r2, userBlockId, userVillageId);
                    return Integer.compare(score2, score1);
                })
                .collect(Collectors.toList());
    }

    /**
     * Calculate location match score
     * Higher score = better match
     */
    private int calculateLocationScore(Map<String, Object> beneficiary,
            Integer userBlockId,
            Integer userVillageId) {
        int score = 0;

        try {
            Map<String, Object> demographics = (Map<String, Object>) beneficiary.get("i_bendemographics");
            if (demographics == null) {
                return score;
            }

            Integer currBlockId = getIntegerFromMap(demographics, "blockID");
            Integer currVillageId = getIntegerFromMap(demographics, "m_districtblock", "blockID");

            // Village match (highest priority) - score: 100
            if (userVillageId != null && userVillageId.equals(currVillageId)) {
                score += 100;
            }

            // Block match - score: 50
            if (userBlockId != null && userBlockId.equals(currBlockId)) {
                score += 50;
            }

            Integer permBlockId = getIntegerFromMap(beneficiary, "permBlockID");
            Integer permVillageId = getIntegerFromMap(beneficiary, "permVillageID");

            if (userVillageId != null && userVillageId.equals(permVillageId)) {
                score += 75;
            }

            if (userBlockId != null && userBlockId.equals(permBlockId)) {
                score += 25;
            }

        } catch (Exception e) {
            logger.error("Error calculating location score: {}", e.getMessage());
        }

        return score;
    }

    /**
     * Helper to safely get Integer from nested maps
     */
    private Integer getIntegerFromMap(Map<String, Object> map, String... keys) {
        Object value = map;
        for (String key : keys) {
            if (value instanceof Map) {
                value = ((Map<String, Object>) value).get(key);
            } else {
                return null;
            }
        }
        return value instanceof Integer ? (Integer) value : null;
    }

    /**
     * Map ES DTO directly to expected API format with COMPLETE data
     */
    private Map<String, Object> mapESResultToExpectedFormat(BeneficiariesESDTO esData) {
        if (esData == null) {
            return null;
        }

        logger.info("ESDATA=" + esData.getAbhaID());

        Map<String, Object> result = new HashMap<>();

        try {
            // Basic fields from ES
            result.put("beneficiaryRegID", esData.getBenRegId());
            result.put("beneficiaryID", esData.getBeneficiaryID());
            result.put("firstName", esData.getFirstName());
            result.put("lastName", esData.getLastName());
            result.put("genderID", esData.getGenderID());
            result.put("genderName", esData.getGenderName());
            result.put("dob", esData.getDOB());
            result.put("dOB", esData.getDOB());
            result.put("age", esData.getAge());
            result.put("fatherName", esData.getFatherName() != null ? esData.getFatherName() : "");
            result.put("spouseName", esData.getSpouseName() != null ? esData.getSpouseName() : "");
            result.put("createdBy", esData.getCreatedBy());
            result.put("createdDate", esData.getCreatedDate());
            result.put("lastModDate", esData.getLastModDate());
            result.put("benAccountID", esData.getBenAccountID());

            result.put("familyID", esData.getFamilyID());

            List<Map<String, Object>> abhaDetails = new ArrayList<>();
            if (esData.getHealthID() != null || esData.getAbhaID() != null) {
                Map<String, Object> abhaDetail = new HashMap<>();
                abhaDetail.put("healthIDNumber", esData.getAbhaID());
                abhaDetail.put("healthID", esData.getAbhaID());
                if (esData.getAbhaCreatedDate() != null) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");

                    LocalDateTime localDateTime = LocalDateTime.parse(esData.getAbhaCreatedDate(), formatter);

                    long createdDateMillis = localDateTime
                            .atZone(ZoneId.of("Asia/Kolkata"))
                            .toInstant()
                            .toEpochMilli();

                    abhaDetail.put("createdDate", createdDateMillis);
                }

                abhaDetail.put("beneficiaryRegID", esData.getBenRegId());
                abhaDetails.add(abhaDetail);
            }
            result.put("abhaDetails", abhaDetails);

            Map<String, Object> mGender = new HashMap<>();
            mGender.put("genderID", esData.getGenderID());
            mGender.put("genderName", esData.getGenderName());
            result.put("m_gender", mGender);

            Map<String, Object> demographics = new HashMap<>();
            demographics.put("beneficiaryRegID", esData.getBenRegId());
            demographics.put("stateID", esData.getStateID());
            demographics.put("stateName", esData.getStateName());
            demographics.put("districtID", esData.getDistrictID());
            demographics.put("districtName", esData.getDistrictName());
            demographics.put("blockID", esData.getBlockID());
            demographics.put("blockName", esData.getBlockName());
            demographics.put("villageID", esData.getVillageID());
            demographics.put("villageName", esData.getVillageName());
            demographics.put("districtBranchID", null);
            demographics.put("districtBranchName", null);
            demographics.put("parkingPlaceID", esData.getParkingPlaceID());
            demographics.put("servicePointID", esData.getServicePointID());
            demographics.put("servicePointName", esData.getServicePointName());
            demographics.put("createdBy", esData.getCreatedBy());

            Map<String, Object> mState = new HashMap<>();
            mState.put("stateID", esData.getStateID());
            mState.put("stateName", esData.getStateName());
            mState.put("stateCode", null);
            mState.put("countryID", 1);
            demographics.put("m_state", mState);

            Map<String, Object> mDistrict = new HashMap<>();
            mDistrict.put("districtID", esData.getDistrictID());
            mDistrict.put("districtName", esData.getDistrictName());
            mDistrict.put("stateID", esData.getStateID());
            demographics.put("m_district", mDistrict);

            Map<String, Object> mBlock = new HashMap<>();
            mBlock.put("blockID", esData.getBlockID());
            mBlock.put("blockName", esData.getBlockName());
            mBlock.put("districtID", esData.getDistrictID());
            mBlock.put("stateID", esData.getStateID());
            demographics.put("m_districtblock", mBlock);

            Map<String, Object> mBranch = new HashMap<>();
            mBranch.put("districtBranchID", null);
            mBranch.put("blockID", esData.getBlockID());
            mBranch.put("villageName", esData.getVillageName());
            mBranch.put("pinCode", esData.getPinCode());
            demographics.put("m_districtbranchmapping", mBranch);

            result.put("i_bendemographics", demographics);

            List<Map<String, Object>> benPhoneMaps = new ArrayList<>();
            if (esData.getPhoneNum() != null && !esData.getPhoneNum().isEmpty()) {
                Map<String, Object> phoneMap = new HashMap<>();
                phoneMap.put("benPhMapID", 1L);
                phoneMap.put("benificiaryRegID", esData.getBenRegId());
                phoneMap.put("parentBenRegID", esData.getBenRegId());
                phoneMap.put("benRelationshipID", 1);
                phoneMap.put("phoneNo", esData.getPhoneNum());

                Map<String, Object> relationType = new HashMap<>();
                relationType.put("benRelationshipID", 1);
                relationType.put("benRelationshipType", "Self");
                phoneMap.put("benRelationshipType", relationType);

                benPhoneMaps.add(phoneMap);
            }
            result.put("benPhoneMaps", benPhoneMaps);

            result.put("isConsent", false);
            result.put("m_title", new HashMap<>());
            result.put("maritalStatus", new HashMap<>());
            result.put("changeInSelfDetails", false);
            result.put("changeInAddress", false);
            result.put("changeInContacts", false);
            result.put("changeInIdentities", false);
            result.put("changeInOtherDetails", false);
            result.put("changeInFamilyDetails", false);
            result.put("changeInAssociations", false);
            result.put("changeInBankDetails", false);
            result.put("changeInBenImage", false);
            result.put("is1097", false);
            result.put("emergencyRegistration", false);
            result.put("passToNurse", false);
            result.put("beneficiaryIdentities", new ArrayList<>());

        } catch (Exception e) {
            logger.error("Error mapping ES result: {}", e.getMessage(), e);
            return null;
        }

        return result;
    }

    /**
     * Direct database search as fallback
     */
    private List<Map<String, Object>> searchInDatabaseDirectly(String query) {
        try {
            List<Object[]> results = benDetailRepo.searchBeneficiaries(query);

            return results.stream()
                    .map(this::mapToExpectedFormat)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("Database search failed: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * Map database result to expected API format
     */
    private Map<String, Object> mapToExpectedFormat(Object[] row) {
        Map<String, Object> result = new HashMap<>();

        try {
            Long beneficiaryRegID = getLong(row[0]);
            String beneficiaryID = getString(row[1]);
            String firstName = getString(row[2]);
            String lastName = getString(row[3]);
            Integer genderID = getInteger(row[4]);
            String genderName = getString(row[5]);
            Date dob = getDate(row[6]);
            Integer age = getInteger(row[7]);
            String fatherName = getString(row[8]);
            String spouseName = getString(row[9]);
            String isHIVPos = getString(row[10]);
            String createdBy = getString(row[11]);
            Date createdDate = getDate(row[12]);
            Long lastModDate = getLong(row[13]);
            Long benAccountID = getLong(row[14]);

            Integer stateID = getInteger(row[15]);
            String stateName = getString(row[16]);
            Integer districtID = getInteger(row[17]);
            String districtName = getString(row[18]);
            Integer blockID = getInteger(row[19]);
            String blockName = getString(row[20]);
            String pinCode = getString(row[21]);
            Integer servicePointID = getInteger(row[22]);
            String servicePointName = getString(row[23]);
            Integer parkingPlaceID = getInteger(row[24]);
            String phoneNum = getString(row[25]);

            result.put("beneficiaryRegID", beneficiaryRegID);
            result.put("beneficiaryID", beneficiaryID);
            result.put("firstName", firstName);
            result.put("lastName", lastName);
            result.put("genderID", genderID);
            result.put("genderName", genderName);
            result.put("dOB", dob);
            result.put("dob", dob);
            result.put("age", age);
            result.put("fatherName", fatherName != null ? fatherName : "");
            result.put("spouseName", spouseName != null ? spouseName : "");
            result.put("createdBy", createdBy);
            result.put("createdDate", createdDate);
            result.put("lastModDate", lastModDate);
            result.put("benAccountID", benAccountID);

            Map<String, Object> mGender = new HashMap<>();
            mGender.put("genderID", genderID);
            mGender.put("genderName", genderName);
            result.put("m_gender", mGender);

            Map<String, Object> demographics = new HashMap<>();
            demographics.put("beneficiaryRegID", beneficiaryRegID);
            demographics.put("stateID", stateID);
            demographics.put("stateName", stateName);
            demographics.put("districtID", districtID);
            demographics.put("districtName", districtName);
            demographics.put("blockID", blockID);
            demographics.put("blockName", blockName);
            demographics.put("districtBranchID", null);
            demographics.put("districtBranchName", null);
            demographics.put("parkingPlaceID", parkingPlaceID);
            demographics.put("servicePointID", servicePointID);
            demographics.put("servicePointName", servicePointName);
            demographics.put("createdBy", createdBy);

            Map<String, Object> mState = new HashMap<>();
            mState.put("stateID", stateID);
            mState.put("stateName", stateName);
            mState.put("stateCode", null);
            mState.put("countryID", 1);
            demographics.put("m_state", mState);

            Map<String, Object> mDistrict = new HashMap<>();
            mDistrict.put("districtID", districtID);
            mDistrict.put("districtName", districtName);
            mDistrict.put("stateID", stateID);
            demographics.put("m_district", mDistrict);

            Map<String, Object> mBlock = new HashMap<>();
            mBlock.put("blockID", blockID);
            mBlock.put("blockName", blockName);
            mBlock.put("districtID", districtID);
            mBlock.put("stateID", stateID);
            demographics.put("m_districtblock", mBlock);

            Map<String, Object> mBranch = new HashMap<>();
            mBranch.put("districtBranchID", null);
            mBranch.put("blockID", blockID);
            mBranch.put("villageName", null);
            mBranch.put("pinCode", pinCode);
            demographics.put("m_districtbranchmapping", mBranch);

            result.put("i_bendemographics", demographics);

            List<Map<String, Object>> benPhoneMaps = fetchPhoneNumbers(beneficiaryRegID);
            result.put("benPhoneMaps", benPhoneMaps);

            result.put("isConsent", false);
            result.put("m_title", new HashMap<>());
            result.put("maritalStatus", new HashMap<>());
            result.put("changeInSelfDetails", false);
            result.put("changeInAddress", false);
            result.put("changeInContacts", false);
            result.put("changeInIdentities", false);
            result.put("changeInOtherDetails", false);
            result.put("changeInFamilyDetails", false);
            result.put("changeInAssociations", false);
            result.put("changeInBankDetails", false);
            result.put("changeInBenImage", false);
            result.put("is1097", false);
            result.put("emergencyRegistration", false);
            result.put("passToNurse", false);
            result.put("beneficiaryIdentities", new ArrayList<>());

        } catch (Exception e) {
            logger.error("Error mapping result: {}", e.getMessage(), e);
        }

        return result;
    }

    /**
     * Fetch phone numbers for a beneficiary
     */
    private List<Map<String, Object>> fetchPhoneNumbers(Long beneficiaryRegID) {
        List<Map<String, Object>> phoneList = new ArrayList<>();

        try {
            List<Object[]> phones = benDetailRepo.findPhoneNumbersByBeneficiaryId(beneficiaryRegID);

            int mapId = 1;
            for (Object[] phone : phones) {
                String phoneNo = getString(phone[0]);
                String phoneType = getString(phone[1]);

                if (phoneNo != null && !phoneNo.isEmpty()) {
                    Map<String, Object> phoneMap = new HashMap<>();
                    phoneMap.put("benPhMapID", (long) mapId++);
                    phoneMap.put("benificiaryRegID", beneficiaryRegID);
                    phoneMap.put("parentBenRegID", beneficiaryRegID);
                    phoneMap.put("benRelationshipID", 1);
                    phoneMap.put("phoneNo", phoneNo);

                    Map<String, Object> relationType = new HashMap<>();
                    relationType.put("benRelationshipID", 1);
                    relationType.put("benRelationshipType", phoneType != null ? phoneType : "Self");
                    phoneMap.put("benRelationshipType", relationType);

                    phoneList.add(phoneMap);
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching phone numbers: {}", e.getMessage(), e);
        }

        return phoneList;
    }

    // Helper methods
    private String getString(Object value) {
        if (value == null)
            return null;
        return value.toString();
    }

    private Long getLong(Object value) {
        if (value == null)
            return null;
        if (value instanceof Long)
            return (Long) value;
        if (value instanceof Integer)
            return ((Integer) value).longValue();
        if (value instanceof BigDecimal)
            return ((BigDecimal) value).longValue();
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Integer getInteger(Object value) {
        if (value == null)
            return null;
        if (value instanceof Integer)
            return (Integer) value;
        if (value instanceof Long)
            return ((Long) value).intValue();
        if (value instanceof BigDecimal)
            return ((BigDecimal) value).intValue();
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Date getDate(Object value) {
        if (value == null)
            return null;
        if (value instanceof Date)
            return (Date) value;
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        if (value instanceof java.sql.Date)
            return new Date(((java.sql.Date) value).getTime());
        return null;
    }
}