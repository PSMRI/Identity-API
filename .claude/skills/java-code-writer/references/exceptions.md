# Custom Exception Classes — AMRIT / FLW-API

## Exception Hierarchy

```
RuntimeException
├── ResourceNotFoundException      → HTTP 404
├── DuplicateResourceException     → HTTP 409
└── InvalidOperationException      → HTTP 422
```

---

## ResourceNotFoundException

Throw when a requested entity does not exist or has been soft-deleted.

```java
package com.piramal.flwapi.exception;

public class ResourceNotFoundException extends RuntimeException {

    private final String resourceName;
    private final String fieldName;
    private final Object fieldValue;

    public ResourceNotFoundException(String resourceName, String fieldName, Object fieldValue) {
        super(String.format("%s not found with %s: '%s'", resourceName, fieldName, fieldValue));
        this.resourceName = resourceName;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public String getResourceName() { return resourceName; }
    public String getFieldName()    { return fieldName; }
    public Object getFieldValue()   { return fieldValue; }
}
```

**Usage:**
```java
exampleRepository.findByIdAndDeletedFalse(id)
    .orElseThrow(() -> new ResourceNotFoundException("AncCare", "id", id));
```

---

## DuplicateResourceException

Throw when a create/update would violate a uniqueness constraint.

```java
package com.piramal.flwapi.exception;

public class DuplicateResourceException extends RuntimeException {

    private final String resourceName;
    private final String fieldName;
    private final Object fieldValue;

    public DuplicateResourceException(String resourceName, String fieldName, Object fieldValue) {
        super(String.format("%s already exists with %s: '%s'", resourceName, fieldName, fieldValue));
        this.resourceName = resourceName;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public String getResourceName() { return resourceName; }
    public String getFieldName()    { return fieldName; }
    public Object getFieldValue()   { return fieldValue; }
}
```

**Usage:**
```java
if (coupleRepository.existsByBeneficiaryIdAndDeletedFalse(request.getBeneficiaryId())) {
    throw new DuplicateResourceException("EligibleCoupleRegister", "beneficiaryId",
        request.getBeneficiaryId());
}
```

---

## InvalidOperationException

Throw when a business rule prevents the requested operation. Use for semantic validation that cannot be expressed as Bean Validation annotations.

```java
package com.piramal.flwapi.exception;

public class InvalidOperationException extends RuntimeException {

    public InvalidOperationException(String message) {
        super(message);
    }

    public InvalidOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

**Usage:**
```java
if (deliveryOutcome.getDeliveryDate().isBefore(ancRecord.getLmpDate())) {
    throw new InvalidOperationException(
        "Delivery date cannot be before last menstrual period date");
}

if (tbTreatment.getStatus() == TreatmentStatus.COMPLETED) {
    throw new InvalidOperationException(
        "Cannot modify a completed TB treatment record");
}
```

---

## HTTP Status Mapping

| Exception | HTTP Status | Spring `@ResponseStatus` | Use case |
|---|---|---|---|
| `ResourceNotFoundException` | 404 Not Found | `HttpStatus.NOT_FOUND` | Entity not found / soft-deleted |
| `DuplicateResourceException` | 409 Conflict | `HttpStatus.CONFLICT` | Unique constraint violation |
| `InvalidOperationException` | 422 Unprocessable Entity | `HttpStatus.UNPROCESSABLE_ENTITY` | Business rule violation |
| `MethodArgumentNotValidException` (Spring) | 400 Bad Request | `HttpStatus.BAD_REQUEST` | Bean Validation failure |
| `AccessDeniedException` (Spring Security) | 403 Forbidden | `HttpStatus.FORBIDDEN` | Missing role/permission |
| `AuthenticationException` (Spring Security) | 401 Unauthorized | `HttpStatus.UNAUTHORIZED` | Invalid/missing JWT |

All are handled in `GlobalExceptionHandler` — see `references/spring-patterns.md` section 9.

---

## Exception Handling Rules

1. **Never swallow exceptions.** An empty `catch` block is a bug.
2. **Always log at the right level:**
   - `WARN` for 4xx errors (expected bad input, not-found).
   - `ERROR` for 5xx errors (unexpected failures).
3. **Never log PII** in exception messages or log statements.
4. **Do not expose stack traces** to API consumers — let `GlobalExceptionHandler` normalize the response.
5. **Wrap checked exceptions** from external libraries (e.g., `IOException` from crypto) in `InvalidOperationException` with a meaningful message.

```java
// Wrong
try {
    return cryptoUtil.decrypt(value);
} catch (Exception e) {
    // swallowed!
}

// Correct
try {
    return cryptoUtil.decrypt(value);
} catch (CryptoException e) {
    log.error("Failed to decrypt field for record id={}", recordId, e);
    throw new InvalidOperationException("Data decryption failed", e);
}
```
