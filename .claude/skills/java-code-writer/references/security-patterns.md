# Security Patterns — AMRIT / FLW-API

## AMRIT Role Constants

These roles are used in `@PreAuthorize` annotations. Always reference constants — never inline strings.

```java
package com.piramal.flwapi.masterEnum;

public final class AmritRoles {
    private AmritRoles() {}

    public static final String ASHA       = "ROLE_ASHA";
    public static final String ANM        = "ROLE_ANM";
    public static final String SUPERVISOR = "ROLE_SUPERVISOR";
    public static final String MO         = "ROLE_MO";          // Medical Officer
    public static final String ADMIN      = "ROLE_ADMIN";
    public static final String VIEWER     = "ROLE_VIEWER";
}
```

---

## @PreAuthorize Patterns

```java
// Single role
@PreAuthorize("hasRole('ROLE_ADMIN')")

// Multiple roles (OR)
@PreAuthorize("hasAnyRole('ROLE_ASHA', 'ROLE_ANM')")

// Role AND ownership check (use with Principal injection)
@PreAuthorize("hasRole('ROLE_SUPERVISOR') or #userId == authentication.principal.id")

// Method-level expression using SpEL
@PreAuthorize("hasRole('ROLE_ADMIN') and #request.districtId != null")
```

**Recommended per endpoint type:**

| Endpoint type | Minimum roles |
|---|---|
| Create / Update beneficiary data | `ROLE_ASHA`, `ROLE_ANM` |
| Read beneficiary data | `ROLE_ASHA`, `ROLE_ANM`, `ROLE_SUPERVISOR` |
| Delete / soft-delete | `ROLE_SUPERVISOR`, `ROLE_ADMIN` |
| Reports / dashboards | `ROLE_SUPERVISOR`, `ROLE_MO`, `ROLE_ADMIN` |
| Admin operations | `ROLE_ADMIN` only |

---

## JWT Filter (reference — already implemented)

The `JwtUserIdValidationFilter` intercepts every request, validates the Bearer token via `JwtUtil`, and populates `SecurityContextHolder`. You do not need to re-implement this.

```java
// JwtUtil — key methods already available
String extractUsername(String token);
boolean validateToken(String token, UserDetails userDetails);
String extractUserId(String token);
```

To extract the current user in a service:

```java
// Inject via controller and pass as parameter — preferred
public ExampleResponse create(CreateExampleRequest request, String currentUser) { ... }

// Or read from SecurityContext directly in service (acceptable)
String currentUser = SecurityContextHolder.getContext()
    .getAuthentication()
    .getName();
```

---

## Spring Security Configuration Template

```java
package com.piramal.flwapi.config;

import com.piramal.flwapi.utils.JwtUserIdValidationFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity          // enables @PreAuthorize
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtUserIdValidationFilter jwtFilter;

    private static final String[] PUBLIC_PATHS = {
        "/actuator/health",
        "/swagger-ui/**",
        "/v3/api-docs/**",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh"
    };

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(PUBLIC_PATHS).permitAll()
                .anyRequest().authenticated()
            )
            .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class)
            .build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(12);
    }
}
```

---

## BCrypt Usage

```java
// Hashing (e.g., on user creation)
String hashed = passwordEncoder.encode(rawPassword);

// Verification (e.g., on login)
boolean matches = passwordEncoder.matches(rawPassword, storedHash);
```

Never store or log raw passwords. Never use MD5 or SHA-1 for password hashing.

---

## Sensitive Data Encryption via CryptoUtil

Fields containing PII (Aadhaar, phone, address) must be encrypted at rest using the existing `CryptoUtil`.

```java
// In service layer — encrypt before save
entity.setAadhaarNumber(cryptoUtil.encrypt(request.getAadhaarNumber()));

// Decrypt on read
response.setAadhaarNumber(cryptoUtil.decrypt(entity.getAadhaarNumber()));
```

Never log the raw (unencrypted) value of any PII field.

---

## Input Validation Security

Rely on Bean Validation (`@Valid`) at the controller boundary. For free-text fields that may be persisted or displayed:

```java
// DTO field — prevent XSS via length limit and pattern
@Pattern(regexp = "^[a-zA-Z0-9 .,\\-]*$", message = "Invalid characters in remarks")
@Size(max = 500)
private String remarks;
```

For file uploads (not currently used but may be added):
- Always validate MIME type server-side, not just client-supplied `Content-Type`.
- Store uploads outside the web root.

---

## No Hardcoded Secrets

```java
// WRONG
private static final String SECRET_KEY = "my-super-secret-123";

// CORRECT — read from application.properties
@Value("${jwt.secret}")
private String jwtSecret;
```

Secrets in `application.properties` must reference environment variables in deployed environments:
```properties
jwt.secret=${JWT_SECRET}
spring.datasource.password=${DB_PASSWORD}
```

Never commit `common_local.properties` with real credentials.
