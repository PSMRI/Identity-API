# 10 Common AMRIT Anti-Patterns

Each pattern shows the wrong code found in the wild, the correct replacement, and the rule it violates.

---

## 1. Business Logic in Controller

**Rule:** Controllers are thin. They validate input, call one service method, and return `ApiResponse`. All `if/else`, calculations, and DB access belong in the service.

**Wrong:**
```java
@PostMapping("/saveEligibleCouple")
public ResponseEntity<?> saveEligibleCouple(
        @RequestBody EligibleCoupleRequest request,
        @RequestHeader("Authorization") String auth) {

    // Business logic directly in controller
    if (request.getHusbandAge() < 18 || request.getWifeAge() < 18) {
        return ResponseEntity.badRequest()
            .body(new ApiResponse(false, "Age below minimum", null));
    }
    EligibleCoupleRegister entity = new EligibleCoupleRegister();
    entity.setBeneficiaryId(request.getBeneficiaryId());
    entity.setHusbandAge(request.getHusbandAge());
    coupleRepo.save(entity);  // direct repo call
    return ResponseEntity.ok(new ApiResponse(true, "Saved", null));
}
```

**Correct:**
```java
@PostMapping("/saveEligibleCouple")
public ResponseEntity<ApiResponse> saveEligibleCouple(
        @Valid @RequestBody EligibleCoupleRequest request,
        @RequestHeader("Authorization") String Authorization) {
    try {
        EligibleCoupleResponse saved = coupleService.saveEligibleCouple(request);
        return ResponseEntity.ok(new ApiResponse(true, "Saved successfully", saved));
    } catch (Exception e) {
        logger.error("Error saving eligible couple: {}", e.getMessage(), e);
        return ResponseEntity.internalServerError()
            .body(new ApiResponse(false, e.getMessage(), null));
    }
}
```

---

## 2. Field Injection Instead of Constructor Injection

**Rule:** Use constructor injection. Field injection hides dependencies, prevents immutability, and breaks unit testing without a Spring context.

**Wrong:**
```java
@Service
public class MaternalHealthServiceImpl implements MaternalHealthService {

    @Autowired
    private AncCareRepo ancCareRepo;       // field injection

    @Autowired
    private BeneficiaryRepo beneficiaryRepo;

    @Autowired
    private AncMapper ancMapper;
}
```

**Correct:**
```java
@Service
public class MaternalHealthServiceImpl implements MaternalHealthService {

    private final AncCareRepo ancCareRepo;
    private final BeneficiaryRepo beneficiaryRepo;
    private final AncMapper ancMapper;

    public MaternalHealthServiceImpl(AncCareRepo ancCareRepo,
                                     BeneficiaryRepo beneficiaryRepo,
                                     AncMapper ancMapper) {
        this.ancCareRepo = ancCareRepo;
        this.beneficiaryRepo = beneficiaryRepo;
        this.ancMapper = ancMapper;
    }
}
```

Or with Lombok:
```java
@Service
@RequiredArgsConstructor
public class MaternalHealthServiceImpl implements MaternalHealthService {
    private final AncCareRepo ancCareRepo;
    private final BeneficiaryRepo beneficiaryRepo;
    private final AncMapper ancMapper;
}
```

---

## 3. Logging PII or Health Data

**Rule:** Never log personally identifiable information or health data. Log entity IDs and status codes only. This is a regulatory requirement under India's DPDP Act and AMRIT's data governance policy.

**Wrong:**
```java
logger.info("Saving ANC for beneficiary name={}, aadhaar={}, phone={}, dob={}",
    beneficiary.getName(),
    beneficiary.getAadhaarNo(),
    beneficiary.getPhoneNo(),
    beneficiary.getDob());

logger.debug("TB diagnosis: patient={}, result={}, testDate={}",
    patient.getName(), diagnosisResult, testDate);
```

**Correct:**
```java
logger.debug("Saving ANC record for benefId={}", request.getBeneficiaryId());

logger.debug("TB diagnosis saved for benefId={}, recordId={}",
    beneficiaryId, savedRecord.getId());
```

---

## 4. Missing @Valid on Request Body

**Rule:** Every `@RequestBody` parameter in a mutation endpoint must have `@Valid`. Without it, Bean Validation annotations on the DTO are silently ignored, allowing null/invalid data into the service layer.

**Wrong:**
```java
@PostMapping("/saveCbacDetails")
public ResponseEntity<?> saveCbacDetails(
        @RequestBody CbacDetailsRequest request,   // no @Valid
        @RequestHeader("Authorization") String auth) {
    // request.getBeneficiaryId() could be null — NullPointerException downstream
    return ResponseEntity.ok(cbacService.save(request));
}
```

**Correct:**
```java
@PostMapping("/saveCbacDetails")
public ResponseEntity<ApiResponse> saveCbacDetails(
        @Valid @RequestBody CbacDetailsRequest request,
        @RequestHeader("Authorization") String Authorization) {
    try {
        return ResponseEntity.ok(new ApiResponse(true, "Saved", cbacService.save(request)));
    } catch (Exception e) {
        logger.error("Error saving CBAC details: {}", e.getMessage(), e);
        return ResponseEntity.internalServerError()
            .body(new ApiResponse(false, e.getMessage(), null));
    }
}
```

And the DTO must declare constraints:
```java
public class CbacDetailsRequest {
    @NotNull(message = "beneficiaryId is required")
    @Positive
    private Long beneficiaryId;

    @NotNull(message = "providerServiceMapId is required")
    private Integer providerServiceMapId;
}
```

---

## 5. Swallowing Exceptions Silently

**Rule:** An empty `catch` block is always a bug. At minimum, log and re-throw. Prefer wrapping in a domain exception.

**Wrong:**
```java
public String saveDeliveryOutcome(DeliveryOutcomeRequest request) {
    try {
        DeliveryOutcome entity = mapper.toEntity(request);
        deliveryRepo.save(entity);
        return "Saved";
    } catch (Exception e) {
        // silent — caller never knows it failed
    }
    return null;
}
```

**Correct:**
```java
public DeliveryOutcomeResponse saveDeliveryOutcome(DeliveryOutcomeRequest request) {
    try {
        DeliveryOutcome entity = mapper.toEntity(request);
        DeliveryOutcome saved = deliveryRepo.save(entity);
        return mapper.toResponse(saved);
    } catch (DataIntegrityViolationException e) {
        logger.error("Duplicate delivery outcome for benefId={}", request.getBeneficiaryId(), e);
        throw new DuplicateResourceException("DeliveryOutcome", "beneficiaryId",
            request.getBeneficiaryId());
    } catch (Exception e) {
        logger.error("Unexpected error saving delivery outcome for benefId={}",
            request.getBeneficiaryId(), e);
        throw new RuntimeException("Failed to save delivery outcome", e);
    }
}
```

---

## 6. Returning Raw Entity from Controller

**Rule:** JPA entities must never leave the service layer. Returning an entity from a controller exposes the DB schema, leaks lazy-loaded collections as serialization errors, and may expose sensitive fields.

**Wrong:**
```java
@GetMapping("/getBeneficiary/{id}")
public ResponseEntity<?> getBeneficiary(@PathVariable Long id) {
    BenRegDetail entity = benRepo.findById(id).orElse(null);
    return ResponseEntity.ok(entity);  // raw JPA entity in response
}
```

**Correct:**
```java
@GetMapping("/getBeneficiary/{id}")
public ResponseEntity<ApiResponse> getBeneficiary(
        @PathVariable @Positive Long id,
        @RequestHeader("Authorization") String Authorization) {
    try {
        BeneficiaryResponse response = beneficiaryService.getById(id);
        return ResponseEntity.ok(new ApiResponse(true, "Fetched", response));
    } catch (Exception e) {
        logger.error("Error fetching beneficiary id={}: {}", id, e.getMessage(), e);
        return ResponseEntity.internalServerError()
            .body(new ApiResponse(false, e.getMessage(), null));
    }
}
```

---

## 7. N+1 Query Problem

**Rule:** Never call a repository method inside a loop over a collection. This fires one query per element (N+1 total). Use a `JOIN FETCH` or a batch query instead.

**Wrong:**
```java
// SupervisorDashboardServiceImpl — N+1 pattern found in production
List<IncentiveActivityRecord> records = incentiveRepo.findByDistrictId(districtId);
for (IncentiveActivityRecord record : records) {
    // One DB query per record to get the verifier's name
    String verifierName = userServiceRoleRepo
        .getUserRole(record.getVerifiedByUserId())
        .get(0)
        .getName();
    record.setVerifiedByName(verifierName);
}
```

**Correct:**
```java
// Option 1: fetch all needed user IDs in one batch query
List<Integer> userIds = records.stream()
    .map(IncentiveActivityRecord::getVerifiedByUserId)
    .distinct()
    .collect(Collectors.toList());

Map<Integer, String> userNameMap = userServiceRoleRepo.getUserNamesByIds(userIds)
    .stream()
    .collect(Collectors.toMap(UserServiceRoleDTO::getUserId, UserServiceRoleDTO::getName));

records.forEach(r -> r.setVerifiedByName(userNameMap.get(r.getVerifiedByUserId())));

// Option 2: use a JOIN in the original query to fetch the name alongside the record
@Query("SELECT new com.iemr.flw.dto.IncentiveWithVerifierDTO(i, u.name) " +
       "FROM IncentiveActivityRecord i JOIN UserServiceRole u ON u.userId = i.verifiedByUserId " +
       "WHERE i.districtId = :districtId AND u.userServciceRoleDeleted = false")
List<IncentiveWithVerifierDTO> findWithVerifierByDistrict(@Param("districtId") Integer districtId);
```

---

## 8. Missing @Transactional on Write Methods

**Rule:** Any service method that performs two or more DB writes must be `@Transactional`. Without it, if the second write fails, the first is already committed — leaving data in a corrupted partial state.

**Wrong:**
```java
public String saveAncAndUpdateBeneficiary(AncRequest request) {
    // Two writes — no transaction. If second fails, first is committed.
    ancCareRepo.save(ancMapper.toEntity(request));
    beneficiaryRepo.updateAncStatus(request.getBeneficiaryId(), "ANC_REGISTERED");
    return "Saved";
}
```

**Correct:**
```java
@Transactional
public AncResponse saveAncAndUpdateBeneficiary(AncRequest request) {
    AncCare saved = ancCareRepo.save(ancMapper.toEntity(request));
    beneficiaryRepo.updateAncStatus(request.getBeneficiaryId(), "ANC_REGISTERED");
    // Both roll back atomically if either fails
    return ancMapper.toResponse(saved);
}
```

Also apply `@Transactional(readOnly = true)` to pure read methods:
```java
@Transactional(readOnly = true)
public List<AncResponse> getAllByBeneficiary(Long beneficiaryId) {
    return ancCareRepo.findByBeneficiaryIdAndDeletedFalse(beneficiaryId)
        .stream().map(ancMapper::toResponse).collect(Collectors.toList());
}
```

---

## 9. Hardcoded Secrets or Credentials

**Rule:** No credentials, API keys, tokens, or connection strings in source code. Use `@Value` referencing property keys that themselves reference environment variables in production.

**Wrong:**
```java
private static final String TM_API_KEY = "amrit-tm-key-20240101";
private static final String FHIR_TOKEN = "Bearer eyJhbGciOiJSUzI1NiJ9...";

// In properties file committed to repo:
// spring.datasource.password=Admin@123
// fhir.api.token=Bearer eyJhbGciOiJSUzI1NiJ9...
```

**Correct:**
```java
@Value("${tm.api.key}")
private String tmApiKey;

@Value("${fhir.api.token}")
private String fhirToken;
```

In `common_example.properties` (the committed template):
```properties
tm.api.key=<set-in-environment>
fhir.api.token=<set-in-environment>
spring.datasource.password=<set-in-environment>
```

In production deployment, values come from environment variables or a secrets manager — never from committed property files.

---

## 10. Throwing Generic RuntimeException Instead of Custom Exceptions

**Rule:** Use domain exceptions that communicate intent and map to the correct HTTP status. Raw `RuntimeException` gives a generic 500 to every caller regardless of the actual problem.

**Wrong:**
```java
public AncResponse getAncById(Long id) {
    AncCare record = ancCareRepo.findById(id)
        .orElseThrow(() -> new RuntimeException("Not found"));  // maps to 500
    return ancMapper.toResponse(record);
}

public void validateAge(int age) {
    if (age < 10 || age > 60) {
        throw new RuntimeException("Invalid age");  // generic — no HTTP mapping
    }
}
```

**Correct:**
```java
public AncResponse getAncById(Long id) {
    AncCare record = ancCareRepo.findByIdAndDeletedFalse(id)
        .orElseThrow(() -> new ResourceNotFoundException("AncCare", "id", id));  // 404
    return ancMapper.toResponse(record);
}

public void validateAge(int age) {
    if (age < 10 || age > 60) {
        throw new InvalidOperationException(
            "Age must be between 10 and 60, received: " + age);  // 422
    }
}
```

Custom exception hierarchy:
```
ResourceNotFoundException    → 404 Not Found
DuplicateResourceException   → 409 Conflict
InvalidOperationException    → 422 Unprocessable Entity
```

All handled centrally by `GlobalExceptionHandler` — controllers never catch these.
