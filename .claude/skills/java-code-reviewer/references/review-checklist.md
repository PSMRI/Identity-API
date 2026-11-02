# Per-Class Review Checklist — AMRIT / FLW-API

Use this checklist when reviewing each file. Mark each item: ✅ PASS / ❌ FAIL / ➖ N/A

---

## Controller

> Package: `com.iemr.flw.controller`
> Naming: `<Domain>Controller`

| # | Item | Severity if Failed |
|---|---|---|
| C-01 | Class annotated `@RestController` and `@RequestMapping(value = "/<path>", headers = "Authorization")` | 🟡 |
| C-02 | All dependencies injected via constructor — no `@Autowired` on fields | 🟡 |
| C-03 | Every method that accepts `@RequestBody` has `@Valid` on that parameter | 🔴 |
| C-04 | Every `@PathVariable` / `@RequestParam` accepting an ID has `@Positive` or `@Min(1)` | 🟡 |
| C-05 | Every method declares `@RequestHeader("Authorization") String Authorization` | 🟡 |
| C-06 | **Zero business logic** in the method body — at most: call one service method, return `ApiResponse` | 🔴 |
| C-07 | No direct repository imports or calls | 🔴 |
| C-08 | No JPA entity type in return signature — only DTOs or `ApiResponse<DTO>` | 🔴 |
| C-09 | Returns `ResponseEntity<ApiResponse<T>>` consistently (not `ResponseEntity<?>` or raw type) | 🟡 |
| C-10 | HTTP method semantics: `@PostMapping` for writes, `@GetMapping` for reads | 🟡 |
| C-11 | SLF4J logger: `private final Logger logger = LoggerFactory.getLogger(...)` | 🟡 |
| C-12 | No `System.out.println` or `e.printStackTrace()` | 🟡 |
| C-13 | No PII logged (no name, Aadhaar, phone, address in log statements) | 🔴 |
| C-14 | `catch` block logs the exception with `logger.error(...)` and returns an error `ApiResponse` — not empty | 🔴 |
| C-15 | `@Operation(summary = "...")` present for each endpoint (Swagger) | 🟢 |
| C-16 | No hardcoded String values used as role names or status codes — use constants | 🟢 |

---

## Service Interface

> Package: `com.iemr.flw.service`
> Naming: `<Domain>Service`

| # | Item | Severity if Failed |
|---|---|---|
| S-01 | File is an `interface`, not a class | 🔴 |
| S-02 | Lives in `service/` package (not `service/impl/`) | 🟡 |
| S-03 | Name is `<Domain>Service` — not `I<Domain>Service` or `<Domain>ServiceInterface` | 🟢 |
| S-04 | All method parameters and return types are DTOs — never JPA entities | 🔴 |
| S-05 | No Spring annotations on the interface (`@Service`, `@Transactional`, etc.) | 🟡 |
| S-06 | No `throws Exception` — declare specific checked exceptions if any | 🟢 |

---

## Service Implementation

> Package: `com.iemr.flw.service.impl`
> Naming: `<Domain>ServiceImpl`

| # | Item | Severity if Failed |
|---|---|---|
| SI-01 | Annotated `@Service` | 🟡 |
| SI-02 | Lives in `service/impl/` package | 🟡 |
| SI-03 | Declares `implements <Domain>Service` | 🔴 |
| SI-04 | All dependencies injected via constructor — no `@Autowired` on fields | 🟡 |
| SI-05 | Every write method annotated `@Transactional` | 🔴 |
| SI-06 | Every read-only method annotated `@Transactional(readOnly = true)` | 🟡 |
| SI-07 | No empty `catch` blocks — every catch logs and re-throws or wraps | 🔴 |
| SI-08 | Throws domain exceptions (`ResourceNotFoundException`, `DuplicateResourceException`, `InvalidOperationException`) — not raw `RuntimeException` | 🟡 |
| SI-09 | SLF4J logger declared `private static final Logger log = LoggerFactory.getLogger(...)` | 🟡 |
| SI-10 | Debug log on method entry with non-PII parameters (IDs only) | 🟢 |
| SI-11 | Error log includes exception object as last arg: `log.error("msg id={}", id, e)` | 🟡 |
| SI-12 | No PII in any log statement | 🔴 |
| SI-13 | No Spring MVC imports (`HttpServletRequest`, `HttpServletResponse`) | 🟡 |
| SI-14 | No direct calls to repositories belonging to the wrong DB (iemr vs identity) | 🟡 |
| SI-15 | Sensitive fields encrypted via `CryptoUtil.encrypt()` before save | 🔴 |
| SI-16 | Sensitive fields decrypted via `CryptoUtil.decrypt()` after load | 🔴 |
| SI-17 | No method longer than 50 lines | 🟡 |
| SI-18 | No nesting deeper than 4 levels | 🟢 |

---

## Repository

> Package: `com.iemr.flw.repo.iemr` or `com.iemr.flw.repo.identity`
> Naming: `<Domain>Repo` or `<Domain>Repository`

| # | Item | Severity if Failed |
|---|---|---|
| R-01 | Declared as `interface` extending `JpaRepository<Entity, ID>` | 🔴 |
| R-02 | Annotated `@Repository` | 🟡 |
| R-03 | Lives in `repo/iemr/` for `db_iemr` entities; `repo/identity/` for `db_identity` | 🟡 |
| R-04 | Custom queries use `@Query` with JPQL or named native SQL — never string concatenation | 🔴 |
| R-05 | Named parameters used: `@Param("name") Type name` — not positional `?1` | 🟡 |
| R-06 | Soft-delete queries filter `deleted = false` or `Deleted = 0` where applicable | 🟡 |
| R-07 | No business logic in the repository (no `if`, no loops, no service calls) | 🔴 |
| R-08 | Read-only summary queries use projections or DTO constructors, not full entity loads | 🟢 |
| R-09 | `findAll()` never called without a `WHERE` clause on large tables — use paged queries | 🟡 |

---

## DTO

> Package: `com.iemr.flw.dto.iemr` or `com.iemr.flw.dto.identity`
> Naming: `<Domain>Request` / `<Domain>Response` / `<Domain>DTO`

| # | Item | Severity if Failed |
|---|---|---|
| D-01 | No `@Entity`, `@Table`, or JPA annotations present | 🔴 |
| D-02 | Request DTOs: all required fields annotated `@NotNull` or `@NotBlank` | 🟡 |
| D-03 | `String` fields have `@Size(max = N)` to prevent oversized input | 🟡 |
| D-04 | ID fields annotated `@Positive` or `@Min(1)` | 🟢 |
| D-05 | Date-of-birth / visit-date fields annotated `@PastOrPresent` | 🟢 |
| D-06 | Separate request DTO and response DTO — not the same class for both directions | 🟢 |
| D-07 | No circular references (DTO → DTO → same DTO) | 🟡 |
| D-08 | Sensitive fields (`aadhaarNumber`, `phoneNumber`) not included in response DTOs that go to lower-privilege clients | 🔴 |
| D-09 | Annotated `@Data` (Lombok) or has explicit getters/setters | 🟡 |

---

## Entity

> Package: `com.iemr.flw.domain.iemr` or `com.iemr.flw.domain.identity`
> Naming: Plain domain noun (e.g., `AncCare`, `ChildRegister`)

| # | Item | Severity if Failed |
|---|---|---|
| E-01 | Annotated `@Entity` and `@Table(name = "t_<domain>", schema = "db_iemr")` | 🟡 |
| E-02 | Lives in the correct package for its database | 🟡 |
| E-03 | Primary key: `@Id` + `@GeneratedValue(strategy = GenerationType.IDENTITY)` | 🟡 |
| E-04 | Soft-delete field present: `Boolean deleted = false` or `Deleted` | 🟡 |
| E-05 | Audit fields present: `createdDate`, `createdBy`, `modifiedDate`, `modifiedBy` | 🟢 |
| E-06 | `@OneToMany` collections use `fetch = FetchType.LAZY` | 🟡 |
| E-07 | `@ManyToMany` collections override the default to `fetch = FetchType.LAZY` | 🟡 |
| E-08 | No business logic in entity (no service calls, no derived compute, no `@PrePersist` with logic) | 🔴 |
| E-09 | No Spring annotations inside entity (no `@Autowired`, `@Value`) | 🔴 |
| E-10 | `toString()` does not include sensitive fields (`aadhaarNumber`, `phoneNumber`) | 🔴 |
| E-11 | `equals()` / `hashCode()` based on `id` only — not mutable fields | 🟡 |

---

## Test Class

> Package: mirrors production package under `src/test/`
> Naming: `<ProductionClass>Test`

| # | Item | Severity if Failed |
|---|---|---|
| T-01 | `@ExtendWith(MockitoExtension.class)` on the class — not `@RunWith(MockitoJUnitRunner.class)` | 🟡 |
| T-02 | `@DisplayName("...")` on the test class — describes the class under test | 🟢 |
| T-03 | Every dependency is `@Mock`; class under test is `@InjectMocks` | 🟡 |
| T-04 | Every `@Test` method has `@DisplayName("should <behaviour> when <condition>")` | 🟢 |
| T-05 | AAA pattern: Arrange → blank line → Act → blank line → Assert | 🟡 |
| T-06 | Happy-path test exists for every public service method | 🟡 |
| T-07 | Not-found / empty path tested with `assertThrows(ResourceNotFoundException.class, ...)` | 🟡 |
| T-08 | Exception message also asserted inside `assertThrows` — not just the exception type | 🟢 |
| T-09 | Assertions are specific: `assertEquals`, `assertThat`, etc. — not just `assertNotNull` | 🟡 |
| T-10 | `verify(mockRepo).save(any())` used when a save side-effect must be confirmed | 🟢 |
| T-11 | No `Thread.sleep` | 🔴 |
| T-12 | No real PII in test fixtures (no real Aadhaar 12-digit numbers, no real phone numbers) | 🔴 |
| T-13 | Test does not access a real database or Redis — all dependencies are mocked | 🔴 |
| T-14 | No shared mutable state between test methods without `@BeforeEach` reset | 🟡 |
| T-15 | `@BeforeEach` used for common setup — not repeated inside each test | 🟢 |
