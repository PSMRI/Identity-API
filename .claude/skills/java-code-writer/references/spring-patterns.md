# Spring Boot Code Templates — AMRIT / FLW-API

## 1. Request DTO

```java
package com.piramal.flwapi.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

@Data
public class CreateExampleRequest {

    @NotNull(message = "beneficiaryId is required")
    @Positive
    private Long beneficiaryId;

    @NotBlank(message = "visitType is required")
    @Size(max = 100)
    private String visitType;

    @PastOrPresent(message = "visitDate must not be in the future")
    private LocalDate visitDate;
}
```

## 2. Response DTO

```java
package com.piramal.flwapi.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ExampleResponse {
    private Long id;
    private Long beneficiaryId;
    private String visitType;
    private LocalDate visitDate;
    private LocalDateTime createdAt;
    private String createdBy;
}
```

## 3. JPA Entity (db_iemr)

```java
package com.piramal.flwapi.domain.iemr;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "t_example")
@Data
public class ExampleRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "beneficiary_id", nullable = false)
    private Long beneficiaryId;

    @Column(name = "visit_type", length = 100)
    private String visitType;

    @Column(name = "visit_date")
    private LocalDate visitDate;

    @Column(name = "deleted", nullable = false)
    private Boolean deleted = false;

    @CreationTimestamp
    @Column(name = "created_date", updatable = false)
    private LocalDateTime createdDate;

    @Column(name = "created_by", length = 50, updatable = false)
    private String createdBy;

    @UpdateTimestamp
    @Column(name = "last_modified_date")
    private LocalDateTime lastModifiedDate;

    @Column(name = "modified_by", length = 50)
    private String modifiedBy;
}
```

## 4. MapStruct Mapper

```java
package com.piramal.flwapi.mapper;

import com.piramal.flwapi.domain.iemr.ExampleRecord;
import com.piramal.flwapi.dto.CreateExampleRequest;
import com.piramal.flwapi.dto.ExampleResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ExampleMapper {

    ExampleRecord toEntity(CreateExampleRequest request);

    @Mapping(source = "createdDate", target = "createdAt")
    ExampleResponse toResponse(ExampleRecord entity);
}
```

## 5. Repository

```java
package com.piramal.flwapi.repo.iemr;

import com.piramal.flwapi.domain.iemr.ExampleRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ExampleRepository extends JpaRepository<ExampleRecord, Long> {

    List<ExampleRecord> findByBeneficiaryIdAndDeletedFalse(Long beneficiaryId);

    @Query("SELECT e FROM ExampleRecord e WHERE e.beneficiaryId = :benefId AND e.deleted = false ORDER BY e.visitDate DESC")
    List<ExampleRecord> findLatestVisitsByBeneficiary(@Param("benefId") Long beneficiaryId);

    Optional<ExampleRecord> findByIdAndDeletedFalse(Long id);
}
```

## 6. Service Interface

```java
package com.piramal.flwapi.service;

import com.piramal.flwapi.dto.CreateExampleRequest;
import com.piramal.flwapi.dto.ExampleResponse;

import java.util.List;

public interface ExampleService {

    ExampleResponse create(CreateExampleRequest request, String createdBy);

    List<ExampleResponse> getAllByBeneficiary(Long beneficiaryId);

    ExampleResponse getById(Long id);

    ExampleResponse update(Long id, CreateExampleRequest request, String modifiedBy);

    void delete(Long id);
}
```

## 7. Service Implementation

```java
package com.piramal.flwapi.service.impl;

import com.piramal.flwapi.domain.iemr.ExampleRecord;
import com.piramal.flwapi.dto.CreateExampleRequest;
import com.piramal.flwapi.dto.ExampleResponse;
import com.piramal.flwapi.exception.ResourceNotFoundException;
import com.piramal.flwapi.mapper.ExampleMapper;
import com.piramal.flwapi.repo.iemr.ExampleRepository;
import com.piramal.flwapi.service.ExampleService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ExampleServiceImpl implements ExampleService {

    private static final Logger log = LoggerFactory.getLogger(ExampleServiceImpl.class);

    private final ExampleRepository exampleRepository;
    private final ExampleMapper exampleMapper;

    @Override
    @Transactional
    public ExampleResponse create(CreateExampleRequest request, String createdBy) {
        log.debug("Creating example record for benefId={}", request.getBeneficiaryId());
        ExampleRecord entity = exampleMapper.toEntity(request);
        entity.setCreatedBy(createdBy);
        ExampleRecord saved = exampleRepository.save(entity);
        log.debug("Created example record id={}", saved.getId());
        return exampleMapper.toResponse(saved);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ExampleResponse> getAllByBeneficiary(Long beneficiaryId) {
        log.debug("Fetching example records for benefId={}", beneficiaryId);
        return exampleRepository.findByBeneficiaryIdAndDeletedFalse(beneficiaryId)
                .stream()
                .map(exampleMapper::toResponse)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(readOnly = true)
    public ExampleResponse getById(Long id) {
        ExampleRecord record = exampleRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException("ExampleRecord", "id", id));
        return exampleMapper.toResponse(record);
    }

    @Override
    @Transactional
    public ExampleResponse update(Long id, CreateExampleRequest request, String modifiedBy) {
        log.debug("Updating example record id={}", id);
        ExampleRecord record = exampleRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException("ExampleRecord", "id", id));
        record.setVisitType(request.getVisitType());
        record.setVisitDate(request.getVisitDate());
        record.setModifiedBy(modifiedBy);
        return exampleMapper.toResponse(exampleRepository.save(record));
    }

    @Override
    @Transactional
    public void delete(Long id) {
        log.debug("Soft-deleting example record id={}", id);
        ExampleRecord record = exampleRepository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ResourceNotFoundException("ExampleRecord", "id", id));
        record.setDeleted(true);
        exampleRepository.save(record);
    }
}
```

## 8. Controller

```java
package com.piramal.flwapi.controller;

import com.piramal.flwapi.dto.CreateExampleRequest;
import com.piramal.flwapi.dto.ExampleResponse;
import com.piramal.flwapi.service.ExampleService;
import com.piramal.flwapi.utils.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/example")
@RequiredArgsConstructor
public class ExampleController {

    private final ExampleService exampleService;

    @PostMapping
    @PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM')")
    public ResponseEntity<ApiResponse<ExampleResponse>> create(
            @Valid @RequestBody CreateExampleRequest request,
            @RequestHeader("Authorization") String authHeader) {
        // Extract user from JWT via JwtUtil if needed, or inject Principal
        ExampleResponse response = exampleService.create(request, "currentUser");
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success("Record created", response));
    }

    @GetMapping("/beneficiary/{beneficiaryId}")
    @PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM', 'ROLE_SUPERVISOR')")
    public ResponseEntity<ApiResponse<List<ExampleResponse>>> getAllByBeneficiary(
            @PathVariable @Positive Long beneficiaryId) {
        List<ExampleResponse> records = exampleService.getAllByBeneficiary(beneficiaryId);
        return ResponseEntity.ok(ApiResponse.success("Records fetched", records));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM', 'ROLE_SUPERVISOR')")
    public ResponseEntity<ApiResponse<ExampleResponse>> getById(@PathVariable @Positive Long id) {
        return ResponseEntity.ok(ApiResponse.success("Record fetched", exampleService.getById(id)));
    }

    @PostMapping("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM')")
    public ResponseEntity<ApiResponse<ExampleResponse>> update(
            @PathVariable @Positive Long id,
            @Valid @RequestBody CreateExampleRequest request) {
        return ResponseEntity.ok(ApiResponse.success("Record updated", exampleService.update(id, request, "currentUser")));
    }

    @PostMapping("/{id}/delete")
    @PreAuthorize("hasAnyRole('ROLE_SUPERVISOR', 'ROLE_ADMIN')")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable @Positive Long id) {
        exampleService.delete(id);
        return ResponseEntity.ok(ApiResponse.success("Record deleted", null));
    }
}
```

## 9. GlobalExceptionHandler

```java
package com.piramal.flwapi.exception;

import com.piramal.flwapi.utils.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiResponse<Void>> handleNotFound(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ApiResponse.error(ex.getMessage()));
    }

    @ExceptionHandler(DuplicateResourceException.class)
    public ResponseEntity<ApiResponse<Void>> handleDuplicate(DuplicateResourceException ex) {
        log.warn("Duplicate resource: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ApiResponse.error(ex.getMessage()));
    }

    @ExceptionHandler(InvalidOperationException.class)
    public ResponseEntity<ApiResponse<Void>> handleInvalidOp(InvalidOperationException ex) {
        log.warn("Invalid operation: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(ApiResponse.error(ex.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Map<String, String>>> handleValidation(
            MethodArgumentNotValidException ex) {
        Map<String, String> errors = ex.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(FieldError::getField, FieldError::getDefaultMessage,
                        (a, b) -> a));
        log.warn("Validation failed: {}", errors);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("Validation failed", errors));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGeneric(Exception ex) {
        log.error("Unexpected error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("An unexpected error occurred"));
    }
}
```

## 10. Pagination Pattern

```java
// Repository
Page<ExampleRecord> findByBeneficiaryIdAndDeletedFalse(Long beneficiaryId, Pageable pageable);

// Service
public Page<ExampleResponse> getAllPaged(Long beneficiaryId, int page, int size) {
    Pageable pageable = PageRequest.of(page, size, Sort.by("visitDate").descending());
    return exampleRepository.findByBeneficiaryIdAndDeletedFalse(beneficiaryId, pageable)
            .map(exampleMapper::toResponse);
}

// Controller
@GetMapping("/beneficiary/{beneficiaryId}/paged")
@PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM', 'ROLE_SUPERVISOR')")
public ResponseEntity<ApiResponse<Page<ExampleResponse>>> getPaged(
        @PathVariable Long beneficiaryId,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "10") int size) {
    return ResponseEntity.ok(ApiResponse.success("Records fetched",
            exampleService.getAllPaged(beneficiaryId, page, size)));
}
```

---

## 11. Factory Pattern — Integration Service (canonical template)

Use this structure for any feature that talks to an external system or has multiple possible implementations.

### 11a. Provider Enum

```java
package com.piramal.flwapi.integration;

public enum IntegrationProvider {
    SAP_ERP,
    ORACLE_ERP,
    LIMS_CONNECT,
    MOCK   // always present — used in tests and local dev
}
```

### 11b. Provider Interface

```java
package com.piramal.flwapi.integration;

public interface IntegrationService {

    /** Key used by the factory to index this implementation. */
    IntegrationProvider getProvider();

    IntegrationResponse send(IntegrationPayload payload);
}
```

### 11c. Concrete Implementation

```java
package com.piramal.flwapi.integration.impl;

import com.piramal.flwapi.integration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SapErpIntegrationService implements IntegrationService {

    private static final Logger log = LoggerFactory.getLogger(SapErpIntegrationService.class);

    @Override
    public IntegrationProvider getProvider() { return IntegrationProvider.SAP_ERP; }

    @Override
    public IntegrationResponse send(IntegrationPayload payload) {
        log.debug("Dispatching to SAP ERP, requestId={}", payload.getRequestId());
        // SAP-specific HTTP/RFC logic
        return IntegrationResponse.ok("SAP_ACK_" + payload.getRequestId());
    }
}
```

### 11d. Mock Implementation (always required)

```java
package com.piramal.flwapi.integration.impl;

import com.piramal.flwapi.integration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MockIntegrationService implements IntegrationService {

    private static final Logger log = LoggerFactory.getLogger(MockIntegrationService.class);

    @Override
    public IntegrationProvider getProvider() { return IntegrationProvider.MOCK; }

    @Override
    public IntegrationResponse send(IntegrationPayload payload) {
        log.debug("Mock integration — no-op for requestId={}", payload.getRequestId());
        return IntegrationResponse.ok("MOCK_OK");
    }
}
```

### 11e. Factory

```java
package com.piramal.flwapi.integration;

import com.piramal.flwapi.exception.InvalidOperationException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class IntegrationServiceFactory {

    private final Map<IntegrationProvider, IntegrationService> registry;

    public IntegrationServiceFactory(List<IntegrationService> services) {
        this.registry = services.stream()
                .collect(Collectors.toMap(IntegrationService::getProvider, Function.identity()));
    }

    public IntegrationService getService(IntegrationProvider provider) {
        IntegrationService service = registry.get(provider);
        if (service == null) {
            throw new InvalidOperationException(
                    "No IntegrationService registered for provider: " + provider);
        }
        return service;
    }
}
```

### 11f. Config-driven Resolver

```java
package com.piramal.flwapi.integration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IntegrationServiceResolver {

    private final IntegrationService activeService;

    public IntegrationServiceResolver(
            IntegrationServiceFactory factory,
            @Value("${integration.provider}") String providerKey) {
        this.activeService = factory.getService(IntegrationProvider.valueOf(providerKey));
    }

    public IntegrationService get() { return activeService; }
}
```

`application.properties`:
```properties
integration.provider=${INTEGRATION_PROVIDER:MOCK}
```

### 11g. Factory Unit Test

```java
@DisplayName("IntegrationServiceFactory")
class IntegrationServiceFactoryTest {

    private IntegrationServiceFactory factory;

    @BeforeEach
    void setUp() {
        factory = new IntegrationServiceFactory(
                List.of(new SapErpIntegrationService(), new MockIntegrationService()));
    }

    @Test
    @DisplayName("returns correct implementation for SAP_ERP provider")
    void returnsSapService() {
        assertThat(factory.getService(IntegrationProvider.SAP_ERP))
                .isInstanceOf(SapErpIntegrationService.class);
    }

    @Test
    @DisplayName("throws InvalidOperationException when provider has no registered implementation")
    void throwsForUnknownProvider() {
        IntegrationServiceFactory empty = new IntegrationServiceFactory(List.of());
        assertThatThrownBy(() -> empty.getService(IntegrationProvider.SAP_ERP))
                .isInstanceOf(InvalidOperationException.class)
                .hasMessageContaining("SAP_ERP");
    }
}
```
