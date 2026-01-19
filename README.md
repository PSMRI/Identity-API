# AMRIT - Identity Service 
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)  [![DeepWiki](https://img.shields.io/badge/DeepWiki-PSMRI%2FIdentity--API-blue)](https://deepwiki.com/PSMRI/Identity-API)

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

## Setting Up Commit Hooks

This project uses Git hooks to enforce consistent code quality and commit message standards. Even though this is a Java project, the hooks are powered by Node.js. Follow these steps to set up the hooks locally:

### Prerequisites
- Node.js (v14 or later)
- npm (comes with Node.js)

### Setup Steps

1. **Install Node.js and npm**
   - Download and install from [nodejs.org](https://nodejs.org/)
   - Verify installation with:
     ```
     node --version
     npm --version
     ```
2. **Install dependencies**
   - From the project root directory, run:
     ```
     npm ci
     ```
   - This will install all required dependencies including Husky and commitlint
3. **Verify hooks installation**
   - The hooks should be automatically installed by Husky
   - You can verify by checking if the `.husky` directory contains executable hooks
### Commit Message Convention
This project follows a specific commit message format:
- Format: `type(scope): subject`
- Example: `feat(login): add remember me functionality`
Types include:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code changes that neither fix bugs nor add features
- `perf`: Performance improvements
- `test`: Adding or fixing tests
- `build`: Changes to build process or tools
- `ci`: Changes to CI configuration
- `chore`: Other changes (e.g., maintenance tasks, dependencies)
Your commit messages will be automatically validated when you commit, ensuring project consistency.

## Usage
All features have been exposed as REST endpoints. Refer to the SWAGGER API specification for details.

## Filing Issues

If you encounter any issues, bugs, or have feature requests, please file them in the [main AMRIT repository](https://github.com/PSMRI/AMRIT/issues). Centralizing all feedback helps us streamline improvements and address concerns efficiently.  

## Join Our Community

We’d love to have you join our community discussions and get real-time support!  
Join our [Discord server](https://discord.gg/FVQWsf5ENS) to connect with contributors, ask questions, and stay updated.  

