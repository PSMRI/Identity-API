# java-code-writer

Write production-ready Spring Boot code for the AMRIT / FLW-API platform.

## Trigger

Use this skill when asked to:
- Add a new API endpoint or module
- Implement a new service or repository
- Scaffold a full feature (DTO → Entity → Repo → Service → Controller + Test)
- Build an integration with an external system (ERP, LIMS, SMS, notification channel, report format, etc.)

---

## Output Order

Always produce artifacts in this sequence — never skip a layer:

1. **Request/Response DTOs** (`dto/`)
2. **JPA Entity** (`domain/iemr/` or `domain/identity/`)
3. **MapStruct Mapper** (`mapper/`)
4. **Repository interface** (`repo/iemr/` or `repo/identity/`)
5. **Service interface** (`service/`)
6. **Service implementation** (`service/impl/`)
7. **Controller** (`controller/`)
8. **Unit test** for the service impl (`src/test/…`)

Additionally, whenever the feature involves an external integration, multiple output formats, or any behaviour with foreseeable alternative implementations, produce these artifacts **before** the Service layer:

8a. **Provider/Strategy enum** (`integration/` or relevant package)
8b. **Provider/Strategy interface** (same package)
8c. **Concrete implementation(s)** (`integration/impl/` or `service/impl/`)
8d. **Factory `@Component`** (`integration/` or relevant package)
8e. **Resolver `@Component`** (config-driven selector)

See `references/factory-patterns.md` for full templates.

If an artifact already exists, show only the diff (new methods / fields).

---

## Architecture Rules

- **Controllers** are thin: `@Valid` on request body, one service call, return `ApiResponse`. No `if/else` business logic.
- **Services** own all business logic. Implement a named interface. Never import Spring MVC types.
- **Repositories** are Spring Data JPA interfaces. Custom queries use `@Query` with named parameters — no string concatenation.
- **DTOs** cross the controller ↔ service boundary. Entities never leave the service layer.
- **Mappers** use MapStruct (`@Mapper(componentModel = "spring")`). No manual field copying elsewhere.

### Factory Pattern Rules (mandatory for integrations and multi-implementation scenarios)

Apply the Factory pattern whenever a feature has more than one possible implementation or communicates with an external system. **Never couple a service directly to a single concrete integration class.**

| Scenario | Factory to create |
|---|---|
| External system integration (ERP, LIMS, IoT…) | `IntegrationServiceFactory` |
| Report generation (PDF, Excel, CSV…) | `ReportGeneratorFactory` |
| Notification dispatch (email, SMS, push…) | `NotificationProviderFactory` |
| Validation strategy (rule sets per plant/lab…) | `ValidatorFactory` |
| Storage backend (DB, S3, filesystem…) | `StorageServiceFactory` |

Rules:
- **Interface declares `getType()`** — always typed with an enum, never a raw string.
- **Factory is `@Component`** injected with `List<ProviderInterface>` — Spring collects all implementations automatically.
- **Active provider is config-driven**: `@Value("${integration.provider}")` in the Resolver, never hard-coded.
- **`InvalidOperationException`** thrown on unknown key — never return `null`.
- **A `MOCK` implementation must always exist** — local dev and unit tests must never depend on a real external system.
- **Adding a new provider = 2 files only**: new enum constant + new `@Service` class. Factory, Resolver, and callers are untouched.

See `references/factory-patterns.md` for full boilerplate and worked examples.

---

## Injection Pattern

Always use **constructor injection**. Never use `@Autowired` on fields.

```java
private final SomeService someService;

public SomeController(SomeService someService) {
    this.someService = someService;
}
```

For service implementations with multiple dependencies, generate a `@RequiredArgsConstructor` (Lombok) or explicit constructor.

---

## Logging Standards

```java
private static final Logger log = LoggerFactory.getLogger(ClassName.class);
```

- Log method entry at `DEBUG` with non-PII parameters only.
- Log exceptions at `ERROR` with message and cause: `log.error("Failed to send to ERP for requestId={}", requestId, e)`.
- Never log: names, Aadhaar, phone numbers, addresses, or any field tagged sensitive.
- Never use `System.out.println` or `e.printStackTrace()`.

---

## Security

- Add `@Valid` to every `@RequestBody` / `@RequestParam` in controller methods.
- Add `@PreAuthorize("hasRole('ROLE_ASHA')")` (or the appropriate AMRIT role) to every controller method. Use role constants from `masterEnum/` — never inline strings.
- Never hardcode passwords, tokens, or keys. Reference `@Value("${property.name}")` or `Environment`.
- Sensitive entity fields must be encrypted/decrypted via `CryptoUtil`.
- Integration credentials (API keys, endpoints, auth tokens for ERP/LIMS/SMS providers) must be read from `@Value` and must reference environment variables in deployed environments.

---

## Validation

Add Bean Validation annotations to DTO fields:
- `@NotNull`, `@NotBlank`, `@Size`, `@Min`, `@Max`, `@Pattern` as appropriate.
- For ID fields: `@Positive`.
- For date fields: `@PastOrPresent` for date of birth / visit dates.

---

## Exception Handling

- Throw `ResourceNotFoundException` when a record is not found (maps to 404).
- Throw `DuplicateResourceException` for unique constraint violations (maps to 409).
- Throw `InvalidOperationException` for business rule violations and for unknown factory keys (maps to 422).
- Never swallow exceptions in an empty catch block.
- Wrap checked exceptions from external integrations (`IOException`, `HttpClientErrorException`, etc.) in `InvalidOperationException` with a meaningful message and the original cause.
- See `references/exceptions.md` for exception class definitions.

---

## References

| File | Contents |
|---|---|
| `references/spring-patterns.md` | Full boilerplate for every layer (DTO → Entity → Repo → Service → Controller → Test) |
| `references/exceptions.md` | Custom exception classes and HTTP mapping table |
| `references/security-patterns.md` | Role constants, `@PreAuthorize` patterns, JWT filter, CryptoUtil usage |
| `references/factory-patterns.md` | **Factory pattern templates** — IntegrationServiceFactory, ReportGeneratorFactory, NotificationProviderFactory, unit tests |

---

## Checklist Before Finalizing Output

- [ ] Every controller method has `@Valid` and `@PreAuthorize`
- [ ] No entity is returned from a controller
- [ ] Constructor injection used throughout
- [ ] SLF4J logger declared (no System.out)
- [ ] No PII logged
- [ ] Unit test covers happy path, not-found, and exception path
- [ ] `@DisplayName` on test class and every test method
- [ ] No hardcoded secrets
- [ ] **If an integration or multi-impl scenario:** Factory + Resolver + Mock impl present
- [ ] **Provider selection is config-driven** (`application.properties` → env var), not hard-coded
- [ ] **`InvalidOperationException` thrown** (not null returned) when factory key is missing
