# /pr-checklist

Audit the current branch against AMRIT PR standards before opening a pull request. Report every finding — do not skip items that cannot be auto-checked.

## Execution Plan

Run each check in order. For each check, print: `[PASS]`, `[FAIL]`, or `[WARN]` followed by a one-line explanation.

---

### 1. Branch Name

```bash
git branch --show-current
```

- PASS if: matches `feature/AMRIT-\d+-.+`, `bugfix/AMRIT-\d+-.+`, or `hotfix/AMRIT-\d+-.+`
- FAIL if: does not include a Jira ticket ID or uses `main`/`release-*`

---

### 2. Target Branch

```bash
git log --oneline origin/main..HEAD | wc -l
git branch -r | grep 'release-' | sort -V | tail -1
```

- Confirm the PR targets the correct branch (feature/bugfix → latest release branch; hotfix → main).
- WARN if the branch appears to have diverged by more than 50 commits.

---

### 3. Build Passes

```bash
mvn clean package -Denv=example -q 2>&1 | tail -5
```

- PASS if build exits 0.
- FAIL if there are compilation errors.

---

### 4. Tests Pass

```bash
mvn test -DskipTests=false 2>&1 | grep -E 'Tests run|BUILD|FAILURE|ERROR' | tail -20
```

- PASS if `BUILD SUCCESS` and 0 failures/errors.
- FAIL if any test fails or the build errors.

---

### 5. No Hardcoded Secrets

```bash
git diff origin/main...HEAD -- '*.java' '*.properties' '*.yml' '*.yaml' | \
  grep -E '(?i)(password|secret|token|apikey|api_key)\s*=\s*["\x27][^$\{]' || echo "CLEAN"
```

- PASS if output is `CLEAN`.
- FAIL if any matches found — list the file and line.

---

### 6. No System.out / printStackTrace

```bash
git diff origin/main...HEAD -- '*.java' | grep -E '(System\.out\.|e\.printStackTrace)' | head -10 || echo "CLEAN"
```

- PASS if `CLEAN`.
- FAIL if matches found.

---

### 7. No PII in Logs

```bash
git diff origin/main...HEAD -- '*.java' | \
  grep -E 'log\.(info|debug|warn|error).*\b(name|aadhaar|phone|mobile|address|dob)\b' | head -10 || echo "CLEAN"
```

- PASS if `CLEAN`.
- WARN if matches found — manual review required.

---

### 8. @Valid Present on Controller Endpoints

```bash
git diff origin/main...HEAD -- 'src/main/java/**/controller/**/*.java' | \
  grep -E '(@PostMapping|@PutMapping|@PatchMapping)' -A3 | grep -v '@Valid' | head -20 || echo "CLEAN"
```

- WARN if any mutation endpoint parameter is missing `@Valid`.

---

### 9. No Direct Repository Calls in Controllers

```bash
git diff origin/main...HEAD -- 'src/main/java/**/controller/**/*.java' | \
  grep -E 'Repository|\.save\(|\.findBy' | head -10 || echo "CLEAN"
```

- FAIL if controllers import or call repositories directly.

---

### 10. No JPA Entities Returned from Controllers

```bash
git diff origin/main...HEAD -- 'src/main/java/**/controller/**/*.java' | \
  grep -E 'ResponseEntity<(List<)?(AncCare|DiseaseAESJE|CbacDetails|ChildRegister|[A-Z][a-z]+Entity)' | head -10 || echo "CLEAN"
```

- FAIL if an entity type appears as a controller return type.

---

### 11. Tests Added for New Code

```bash
git diff --name-only origin/main...HEAD | grep 'src/main/java' | sed 's|src/main/java/||;s|\.java$||' | \
  while read class; do
    base=$(basename "$class")
    find src/test -name "${base}Test.java" | head -1
  done
```

- WARN for every changed production class that has no corresponding test class.

---

### 12. Constructor Injection Only

```bash
git diff origin/main...HEAD -- '*.java' | grep -E '@Autowired\s+private' | head -10 || echo "CLEAN"
```

- FAIL if field-level `@Autowired` is used.

---

## Summary

After all checks, print a summary table:

| # | Check | Status |
|---|---|---|
| 1 | Branch name | |
| 2 | Target branch | |
| 3 | Build passes | |
| 4 | Tests pass | |
| 5 | No hardcoded secrets | |
| 6 | No System.out | |
| 7 | No PII in logs | |
| 8 | @Valid on endpoints | |
| 9 | No repo calls in controller | |
| 10 | No entities from controller | |
| 11 | Tests added | |
| 12 | Constructor injection | |

Then print: **"Ready to open PR"** if all checks PASS/WARN, or **"PR blocked — fix FAIL items first"** if any FAIL.
