# java-code-reviewer

Orchestrator skill for reviewing Java / Spring Boot code in the AMRIT / FLW-API project.

## Triggers

Activate this skill when the user says any of:
- "review this", "check my code", "audit this", "PR review", "code review"
- "is this correct", "what's wrong with this", "look at this file"
- Pastes raw Java code or a file path to a `.java` file
- Shares a diff or lists 2+ changed files

---

## Routing Decision

### Single file or pasted snippet
Run a full inline review yourself. Work through all 6 dimensions below in order. Produce the structured report defined in **Report Format**.

### PR with 3 or more changed files
Spawn the three specialized agents **in parallel**:
1. `agents/security-auditor.md` — security scan
2. `agents/architecture-reviewer.md` — layering and SOLID check
3. `agents/test-reviewer.md` — test coverage and quality

Wait for all three JSON outputs. Then synthesize them into one final **Synthesized PR Report** (format defined below). Do not produce a partial report while agents are running.

---

## 6 Review Dimensions

Work through every dimension for every file. Skip a dimension only if it is genuinely not applicable (e.g., no tests exist yet and the task is not about testing).

### 1. Architecture & Layering
Enforce the strict AMRIT layer contract:

```
Controller → Service Interface → Service Impl → Repository → Entity
                                       ↕
                                    Mapper (DTO ↔ Entity)
```

Check:
- Controllers import only: DTOs, service interfaces, `ApiResponse`, Spring MVC types, validation annotations. No repository or entity imports.
- Service implementations own all business logic. No Spring MVC types (`HttpServletRequest`, etc.).
- Repositories are Spring Data JPA interfaces only. No business logic.
- DTOs cross the Controller ↔ Service boundary. Entities never leave the service layer.
- Mappers (MapStruct) handle all DTO ↔ Entity conversion. No manual field copying.
- Dependency direction never points upward (lower layer must not import higher layer).

### 2. Security
This project uses **custom JWT auth** (not Spring Security method security). There are no `@PreAuthorize` annotations in the current codebase. The security model is:
- `JwtUserIdValidationFilter` validates the JWT and returns 401 if invalid
- `HTTPRequestInterceptor` validates the token exists in Redis via `Validator.checkKeyExists()`
- Role data lives in `v_userservicerolemapping` (read via `UserServiceRoleRepo`)

Check for:
- Missing `Authorization` header requirement on controller endpoints (all non-public endpoints must declare `@RequestHeader("Authorization")` or have `headers = "Authorization"` on the `@RequestMapping`)
- Missing `@Valid` on any `@RequestBody` parameter
- `@RequestParam` / `@PathVariable` accepting IDs without `@Positive`/`@Min(1)`
- Hardcoded passwords, API keys, tokens, or connection strings in any `.java` or `.properties` file
- PII in log statements: names, Aadhaar, phone numbers, addresses, dates of birth
- `@Query` values built with string concatenation (SQL/JPQL injection)
- Sensitive fields stored without `CryptoUtil.encrypt()` / `CryptoUtil.decrypt()`
- `System.out.println` or `e.printStackTrace()` in production code

### 3. Code Quality
- Field injection (`@Autowired` on fields) — must be constructor injection
- `System.out.println` or `e.printStackTrace()` instead of SLF4J logger
- Empty `catch` blocks (exception swallowing)
- Generic `RuntimeException` thrown instead of domain exceptions (`ResourceNotFoundException`, etc.)
- Overly long methods (> 50 lines) or deeply nested code (> 4 levels)
- Dead code, commented-out blocks
- Naming: classes PascalCase, methods/fields camelCase, constants SCREAMING_SNAKE

### 4. Spring Boot Best Practices
- Write methods missing `@Transactional`
- Read methods missing `@Transactional(readOnly = true)`
- `@OneToMany` / `@ManyToMany` collections using `FetchType.EAGER` (default for `@ManyToMany` is EAGER — must be overridden to LAZY)
- `@OneToMany` without `@BatchSize` or `@EntityGraph` where collections are iterated
- Returning raw entity from a controller instead of a DTO
- `new` construction of Spring beans (bypasses the container)
- `@Value` used on non-`String` fields without type conversion (silent null)

### 5. Testing
- Every public service method must have at least one test
- Tests must use JUnit 5 (`@ExtendWith(MockitoExtension.class)`) — not JUnit 4 `@RunWith`
- `@Mock` for dependencies, `@InjectMocks` for the class under test
- AAA pattern: Arrange / Act / Assert clearly separated by blank lines
- `@DisplayName` on test class and every test method
- Happy path + not-found path + exception path coverage per method
- Assertions must be specific — `assertNotNull` alone is a warning
- `verify()` to confirm side effects (saves, deletes)
- No `Thread.sleep`, no real PII in fixtures

### 6. Performance
- N+1 queries: iterating a collection and calling a repo method per element
- Missing `@Transactional` on multi-step writes (separate commits on failure)
- `findAll()` on a table without a `WHERE` clause on large tables
- `String` concatenation in loops (use `StringBuilder`)
- Repeated `.getSessionObject()` calls inside loops instead of one call outside

---

## Report Format — Single File

```
## Code Review — <ClassName.java>

### Summary
<2–3 sentences: what the code does, overall quality verdict, biggest risk>

---

### 🔴 Critical Issues
<One block per issue:>
**[ARCH-1] <Title>** — `FileName.java:<line>`
> <What is wrong and why it matters>
> Fix: <specific action>

*(None)* — if no criticals

---

### 🟡 Warnings
**[SEC-1] <Title>** — `FileName.java:<line>`
> <What is wrong>
> Fix: <specific action>

*(None)* — if no warnings

---

### 🟢 Suggestions
**[QUAL-1] <Title>** — `FileName.java:<line>`
> <What could be improved>

*(None)* — if no suggestions

---

### ✅ What's Done Well
- <Specific correct pattern observed — e.g., "Constructor injection used throughout">
- <max 3 items>

---

### Verdict

| Dimension            | Status                  |
|----------------------|-------------------------|
| Architecture         | ✅ / 🔴 N critical / 🟡 N warnings |
| Security             | ✅ / 🔴 N critical / 🟡 N warnings |
| Code Quality         | ✅ / 🟡 N warnings / 🟢 N suggestions |
| Spring Boot          | ✅ / 🟡 N warnings      |
| Testing              | ✅ / 🟡 N warnings      |
| Performance          | ✅ / 🟢 N suggestions   |

**APPROVED** — all dimensions pass
**APPROVED WITH COMMENTS** — only warnings/suggestions, no criticals
**CHANGES REQUESTED** — one or more critical issues present
```

---

## Report Format — Synthesized PR Report (3+ files)

After receiving JSON from all three agents, produce:

```
## PR Review — <branch or PR title>

### Summary
<3–4 sentences: what the PR does, total finding counts, merge recommendation>

---

### 🔴 Critical Issues  (<N total>)
<Merge all CRITICAL findings from all three agent outputs.>
**[<ID>] <title>** — `<file>:<line>`
> <description>
> Fix: <recommendation>

---

### 🟡 Warnings  (<N total>)
<Merge all WARNING findings from all three agent outputs.>

---

### 🟢 Suggestions  (<N total>)
<Merge all INFO/SUGGESTION findings.>

---

### Coverage Summary  (from test-reviewer)
| Class | Methods | Fully Covered | Partial | Not Covered |
|---|---|---|---|---|
| <ClassName> | N | N | N | N |

Overall coverage: X / Y methods fully tested (Z%)

---

### Verdict

| Dimension      | Agent               | Status         |
|----------------|---------------------|----------------|
| Security       | security-auditor    | 🔴 / 🟡 / ✅   |
| Architecture   | architecture-reviewer | 🔴 / 🟡 / ✅  |
| Testing        | test-reviewer       | 🟡 / ✅        |

**APPROVED** / **APPROVED WITH COMMENTS** / **CHANGES REQUESTED**
```

---

## Finding ID Prefixes

| Prefix | Dimension |
|---|---|
| `ARCH-n` | Architecture & Layering |
| `SEC-n` | Security |
| `QUAL-n` | Code Quality |
| `BOOT-n` | Spring Boot Best Practice |
| `TEST-n` | Testing |
| `PERF-n` | Performance |

---

## Verdict Rules

| Condition | Verdict |
|---|---|
| Zero criticals, zero warnings | APPROVED |
| Zero criticals, one or more warnings or suggestions | APPROVED WITH COMMENTS |
| One or more criticals | CHANGES REQUESTED |

---

## Reference Files

- `references/review-checklist.md` — per-class-type checklist
- `references/common-issues.md` — 10 AMRIT anti-patterns with wrong/correct examples
- `agents/security-auditor.md` — deep security scan agent
- `agents/architecture-reviewer.md` — layering and SOLID agent
- `agents/test-reviewer.md` — test coverage and quality agent
