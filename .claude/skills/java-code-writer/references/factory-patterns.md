# Factory Design Patterns — AMRIT / FLW-API

Use Factory patterns wherever there are **multiple implementations** of a behaviour (integrations, report formats, notification channels, validators, etc.) so future providers can be plugged in without touching existing code.

---

## When to Apply a Factory

| Scenario | Factory type to use |
|---|---|
| Multiple external integrations (SMS, ERP, LIMS…) | `IntegrationServiceFactory` |
| Multiple report formats (PDF, Excel, CSV…) | `ReportGeneratorFactory` |
| Multiple notification channels (email, push, SMS…) | `NotificationProviderFactory` |
| Multiple validators with shared interface | `ValidatorFactory` |
| Multiple storage backends (DB, S3, local…) | `StorageServiceFactory` |

---

## 1. Provider / Strategy Interface

Every variant implements a common interface that declares a **type identifier** plus the operation contract.

```java
package com.piramal.flwapi.integration;

/**
 * Common contract for all external integration providers.
 * Implement this interface for every new provider — no other class needs to change.
 */
public interface IntegrationService {

    /**
     * Returns the unique provider key used to look this implementation up in
     * {@link IntegrationServiceFactory}.  Must be UPPER_SNAKE_CASE and match an
     * {@code integration.provider} config value.
     */
    IntegrationProvider getProvider();

    /**
     * Sends {@code payload} to the external system.
     *
     * @param payload  the data to transmit (never null)
     * @return         the raw response from the provider
     * @throws IntegrationException on any provider-side error
     */
    IntegrationResponse send(IntegrationPayload payload);
}
```

```java
package com.piramal.flwapi.integration;

/** Enum of all supported integration providers. Add new values here when onboarding a provider. */
public enum IntegrationProvider {
    SAP_ERP,
    ORACLE_ERP,
    LIMS_CONNECT,
    MOCK  // for tests / local dev
}
```

---

## 2. Concrete Implementations

Each implementation is a Spring `@Service` — the factory discovers them automatically through injection.

```java
package com.piramal.flwapi.integration.impl;

import com.piramal.flwapi.integration.IntegrationPayload;
import com.piramal.flwapi.integration.IntegrationProvider;
import com.piramal.flwapi.integration.IntegrationResponse;
import com.piramal.flwapi.integration.IntegrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SapErpIntegrationService implements IntegrationService {

    private static final Logger log = LoggerFactory.getLogger(SapErpIntegrationService.class);

    @Override
    public IntegrationProvider getProvider() {
        return IntegrationProvider.SAP_ERP;
    }

    @Override
    public IntegrationResponse send(IntegrationPayload payload) {
        log.debug("Sending payload to SAP ERP, requestId={}", payload.getRequestId());
        // SAP-specific HTTP/RFC call here
        return IntegrationResponse.ok("SAP_ACK_" + payload.getRequestId());
    }
}
```

```java
package com.piramal.flwapi.integration.impl;

import com.piramal.flwapi.integration.IntegrationPayload;
import com.piramal.flwapi.integration.IntegrationProvider;
import com.piramal.flwapi.integration.IntegrationResponse;
import com.piramal.flwapi.integration.IntegrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Stub used in local / test environments — never wired in production. */
@Service
public class MockIntegrationService implements IntegrationService {

    private static final Logger log = LoggerFactory.getLogger(MockIntegrationService.class);

    @Override
    public IntegrationProvider getProvider() {
        return IntegrationProvider.MOCK;
    }

    @Override
    public IntegrationResponse send(IntegrationPayload payload) {
        log.debug("Mock integration — swallowing payload for requestId={}", payload.getRequestId());
        return IntegrationResponse.ok("MOCK_OK");
    }
}
```

---

## 3. The Factory

The factory is injected with **all** `IntegrationService` beans by Spring and indexes them by provider.

```java
package com.piramal.flwapi.integration;

import com.piramal.flwapi.exception.InvalidOperationException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolves the correct {@link IntegrationService} at runtime based on a configured
 * {@link IntegrationProvider} key.
 *
 * <p>To add a new provider: create a class that implements {@link IntegrationService},
 * annotate it with {@code @Service}, and add its key to {@link IntegrationProvider}.
 * No changes needed here or anywhere else.
 */
@Component
public class IntegrationServiceFactory {

    private final Map<IntegrationProvider, IntegrationService> registry;

    /** Spring injects every IntegrationService bean in the application context. */
    public IntegrationServiceFactory(List<IntegrationService> services) {
        this.registry = services.stream()
                .collect(Collectors.toMap(IntegrationService::getProvider, Function.identity()));
    }

    /**
     * Returns the integration service for the given provider.
     *
     * @param provider the target provider
     * @return the matching service
     * @throws InvalidOperationException if no implementation is registered for the provider
     */
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

---

## 4. Driving the Factory from Config

The active provider is read from `application.properties` — switching providers is a config change, not a code change.

```properties
# application.properties
integration.provider=SAP_ERP
```

```java
package com.piramal.flwapi.integration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Resolves the configured provider once on startup and caches the service.
 * Inject this wherever you need to send to the external system.
 */
@Component
public class IntegrationServiceResolver {

    private final IntegrationService activeService;

    public IntegrationServiceResolver(
            IntegrationServiceFactory factory,
            @Value("${integration.provider}") String providerKey) {

        IntegrationProvider provider = IntegrationProvider.valueOf(providerKey);
        this.activeService = factory.getService(provider);
    }

    public IntegrationService get() {
        return activeService;
    }
}
```

Usage in a service:
```java
@Service
@RequiredArgsConstructor
public class SampleDispatchService {

    private final IntegrationServiceResolver integrationResolver;

    public void dispatchToErp(Long requestId) {
        IntegrationPayload payload = IntegrationPayload.of(requestId);
        IntegrationResponse response = integrationResolver.get().send(payload);
        log.debug("ERP dispatch completed, reference={}", response.getReference());
    }
}
```

---

## 5. Report Generator Factory (second example — same pattern)

```java
// Interface
public interface ReportGenerator {
    ReportFormat getFormat();
    byte[] generate(ReportData data);
}

// Enum
public enum ReportFormat { PDF, EXCEL, CSV }

// Implementation example
@Service
public class PdfReportGenerator implements ReportGenerator {
    @Override public ReportFormat getFormat() { return ReportFormat.PDF; }
    @Override public byte[] generate(ReportData data) { /* iText / JasperReports */ return new byte[0]; }
}

// Factory — identical structure to IntegrationServiceFactory
@Component
public class ReportGeneratorFactory {
    private final Map<ReportFormat, ReportGenerator> registry;
    public ReportGeneratorFactory(List<ReportGenerator> generators) {
        this.registry = generators.stream()
                .collect(Collectors.toMap(ReportGenerator::getFormat, Function.identity()));
    }
    public ReportGenerator getGenerator(ReportFormat format) {
        ReportGenerator generator = registry.get(format);
        if (generator == null) throw new InvalidOperationException("No generator for format: " + format);
        return generator;
    }
}
```

---

## 6. Notification Provider Factory (third example)

```java
// Interface
public interface NotificationProvider {
    NotificationChannel getChannel();
    void send(NotificationPayload payload);
}

// Enum
public enum NotificationChannel { EMAIL, SMS, PUSH }

// EMAIL implementation
@Service
public class EmailNotificationProvider implements NotificationProvider {
    @Override public NotificationChannel getChannel() { return NotificationChannel.EMAIL; }
    @Override public void send(NotificationPayload payload) { /* SMTP / SES */ }
}

// SMS implementation
@Service
public class SmsNotificationProvider implements NotificationProvider {
    @Override public NotificationChannel getChannel() { return NotificationChannel.SMS; }
    @Override public void send(NotificationPayload payload) { /* Twilio / AWS SNS */ }
}

// Factory
@Component
public class NotificationProviderFactory {
    private final Map<NotificationChannel, NotificationProvider> registry;
    public NotificationProviderFactory(List<NotificationProvider> providers) {
        this.registry = providers.stream()
                .collect(Collectors.toMap(NotificationProvider::getChannel, Function.identity()));
    }
    public NotificationProvider getProvider(NotificationChannel channel) {
        NotificationProvider provider = registry.get(channel);
        if (provider == null) throw new InvalidOperationException("No provider for channel: " + channel);
        return provider;
    }
}
```

---

## 7. Unit Testing a Factory

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
    @DisplayName("returns SAP implementation when provider is SAP_ERP")
    void returnsSapService() {
        IntegrationService service = factory.getService(IntegrationProvider.SAP_ERP);
        assertThat(service).isInstanceOf(SapErpIntegrationService.class);
    }

    @Test
    @DisplayName("throws InvalidOperationException for unknown provider")
    void throwsForUnknownProvider() {
        // Force removal to simulate missing implementation
        IntegrationServiceFactory emptyFactory = new IntegrationServiceFactory(List.of());
        assertThatThrownBy(() -> emptyFactory.getService(IntegrationProvider.SAP_ERP))
                .isInstanceOf(InvalidOperationException.class)
                .hasMessageContaining("SAP_ERP");
    }
}
```

---

## Factory Pattern Rules

- **Interface declares `getType()`** — the key method the factory indexes by. Always use an enum, never a string.
- **Factory is `@Component`**, never `@Service` — it orchestrates, it does not implement business logic.
- **Implementations are `@Service`** — Spring auto-discovers them; the factory just collects what's already in the context.
- **Config drives selection** — use `@Value("${…}")` to pick the active provider; never hard-code it.
- **`InvalidOperationException` on missing key** — fail fast with a clear message rather than returning `null`.
- **Mock / stub implementation always exists** — ensures local dev and tests never need a real external system.
- **Adding a new provider = 2 files only**: a new enum constant + a new `@Service` class. Zero changes to factory, resolver, or callers.
