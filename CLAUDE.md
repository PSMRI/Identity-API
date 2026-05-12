# CLAUDE.md - Identity-API

## Project Overview

Identity-API is the beneficiary identity management service for the AMRIT platform. It handles beneficiary creation, search, update, and deduplication across dual database profiles (`db_iemr` main and `db_identity`). It supports RMNCH (Reproductive, Maternal, Newborn, Child, and Adolescent Health) data management, family tagging, Elasticsearch-based beneficiary search, and health ID linkage.

## Tech Stack

- Java 17, Spring Boot 3.2.2, Maven
- Spring Data JPA / Hibernate, MySQL 8.0
- Elasticsearch (Spring Data Elasticsearch for beneficiary search indexing)
- Redis for session management
- Lombok (1.18.36), MapStruct
- SpringDoc OpenAPI (Swagger UI at `/swagger-ui.html`)
- ECS logging (logback-ecs-encoder)
- JaCoCo for test coverage
- Packaged as WAR for Wildfly deployment

## Build & Run

```bash
mvn clean install -DENV_VAR=local          # Build
mvn spring-boot:run -DENV_VAR=local        # Run locally
mvn -B package --file pom.xml -P <profile> # Package WAR (dev, local, test, ci, uat)
mvn test                                    # Run tests
```

Environment config: `src/main/resources/common_<ENV_VAR>.properties` is copied to `application.properties` at build time.

## Key Packages (`com.iemr.common.identity`)

- **controller/** - REST endpoints:
  - `IdentityController` - Core beneficiary CRUD (create, search, update, search by phone/ID/name)
  - `IdentityESController` - Elasticsearch-based beneficiary search
  - `rmnch/RMNCHMobileAppController` - RMNCH mobile app data sync
  - `familyTagging/FamilyTaggingController` - Family tagging and family search
  - `elasticsearch/ElasticsearchSyncController` - Elasticsearch sync management
  - `health/HealthController` - Health check endpoint
  - `version/VersionController` - API version info
- **service/** - Business logic:
  - `IdentityService` - Core identity operations
  - `rmnch/` - RMNCH beneficiary management
  - `familyTagging/` - Family tagging logic
  - `elasticsearch/` - Elasticsearch indexing and sync
  - `health/` - Health check service
- **domain/** - Core JPA entities for beneficiary data:
  - `MBeneficiaryregidmapping` - Beneficiary registration ID mapping
  - `MBeneficiaryaddress`, `MBeneficiarycontact`, `MBeneficiaryAccount` - Beneficiary demographics
  - `MBeneficiaryfamilymapping` - Family relationships
  - `MBeneficiaryconsent` - Consent management
  - `VBenAdvanceSearch` - View for advanced search queries
- **data/** - Additional data models:
  - `rmnch/` - RMNCH-specific entities (CBAC details, born birth details, household details, NCD/TB/HRP data)
  - `elasticsearch/` - Elasticsearch document models and sync job
  - `familyTagging/` - Family tagging models
- **dto/** - Data transfer objects for API requests/responses
- **repo/** - Spring Data JPA and Elasticsearch repositories
- **mapper/** - MapStruct mappers for entity-DTO conversion
- **filter/** - Servlet filters
- **security/** - Security configuration
- **utils/** - Utilities (Redis, HTTP, validation, session, gateway, email, exception handling)
- **config/** - Application configuration

## Architecture Notes

- Dual-profile beneficiary storage: main identity in `db_identity`, with mapping to `db_iemr` for AMRIT platform integration
- Elasticsearch integration provides fast full-text beneficiary search with background sync jobs
- RMNCH module handles field-worker mobile app data (CBAC screening, household surveys, birth details)
- Family tagging enables linking beneficiaries into family units with search by family ID
- MapStruct mappers handle complex entity-to-DTO transformations
- Health ID (ABHA) linkage stored per beneficiary for ABDM integration
- Beneficiary deduplication logic via advanced search views
- Artifact ID: `identity-api`, group: `com.iemr.common.identity`, version: 3.6.1
