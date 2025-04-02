# AMRIT - Identity Service 
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)  ![branch parameter](https://github.com/PSMRI/Identity-API/actions/workflows/sast-and-package.yml/badge.svg)

Identity API is a microservice which is used for the creation and management of beneficaries.

### Primary Features
* Beneficiary Creation
* Beneficiary Search
* Beneficiary Management


## Environment and setup
For setting up the development environment, please refer to the [Developer Guide](https://piramal-swasthya.gitbook.io/amrit/developer-guide/development-environment-setup) .

# Running Instructions

This service consists of two profiles that work together: the main application profile and the 1097_identity profile. Each profile operates on a different port for independent functionality.

## Building and Running

### 1. Main Application (Port: 8094)
```bash
# Build and Run
mvn clean install -DENV_VAR=local
mvn spring-boot:run -DENV_VAR=local
```

### 2. 1097_identity Profile (Port: 8095)
```bash
# Build and Run (in a new terminal)
mvn clean install -P1097_identity -DENV_VAR=local
mvn spring-boot:run -P1097_identity -DENV_VAR=local
```

**Note:** Start the main application before running the 1097_identity profile. Each profile requires the `-DENV_VAR=local` parameter for local development.

## API Guide
Detailed information on API endpoints can be found in the [API Guide](https://piramal-swasthya.gitbook.io/amrit/architecture/api-guide).

## Integrations
* RMNCH (Reproductive, Maternal, Newborn, and Child Health)

## Usage
All features have been exposed as REST endpoints. Refer to the SWAGGER API specification for details.

## Filing Issues

If you encounter any issues, bugs, or have feature requests, please file them in the [main AMRIT repository](https://github.com/PSMRI/AMRIT/issues). Centralizing all feedback helps us streamline improvements and address concerns efficiently.  

## Join Our Community

We’d love to have you join our community discussions and get real-time support!  
Join our [Discord server](https://discord.gg/FVQWsf5ENS) to connect with contributors, ask questions, and stay updated.  

