# Project Structure Migration Plan

## Target Structure
```
project_recruitment/
│
├── pyproject.toml          # single source‑of‑truth for build & deps (PEP 517)
├── README.md
├── .env.example            # show required env vars; real .env in Secrets
├── docker-compose.yml
├── docker/
│   ├── discovery.Dockerfile
│   └── processing.Dockerfile
│
├── src/                    # importable code lives ONLY here  ← key change
│   └── recruitment/
│       ├── __init__.py
│       ├── logging_config.py
│       ├── config.py       # Pydantic/BaseSettings → pulls from env
│       ├── db/
│       │   ├── __init__.py
│       │   ├── models.py
│       │   ├── migrations/         # (Alembic or SQL files)
│       │   └── repository.py
│       ├── services/
│       │   ├── discovery/
│       │   │   ├── __init__.py
│       │   │   └── main.py
│       │   └── processing/
│       │       ├── __init__.py
│       │       └── main.py
│       ├── workers/                # background consumers, schedulers
│       ├── utils/
│       │   └── __init__.py
│       └── prompts/                # prompt text or templates
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
├── scripts/                # one‑off CLIs (populate_queue.py, etc.)
└── .gitignore              # **logs/**  **databases/**  *.db  *.bak …
```

## Current State Analysis

### Existing Files and Directories
- Core project files exist (`pyproject.toml`, `README.md`, `docker-compose.yml`)
- Some target directories already exist (`src/`, `docker/`, `scripts/`, `tests/`)
- Multiple Python files in root directory that need to be moved
- Existing services directory that needs restructuring
- Multiple configuration files (`requirements.txt`, `setup.py`, `pyproject.toml`)

### Potential Issues and Considerations

1. **Configuration Management**
   - Multiple dependency management files (`requirements.txt`, `setup.py`, `pyproject.toml`)
   - Need to consolidate to single source of truth using `pyproject.toml`

2. **Code Organization**
   - Python files in root directory need to be moved to appropriate locations
   - Existing `recruitment/` directory needs to be restructured
   - Services need to be reorganized into the new structure

3. **Environment Configuration**
   - No `.env.example` file currently exists
   - Need to identify and document required environment variables

4. **Testing Structure**
   - Existing tests need to be reorganized into unit/integration/e2e
   - `tests_archive/` directory needs to be reviewed and potentially merged

5. **Docker Configuration**
   - Docker directory exists but needs verification of Dockerfile structure
   - Need to ensure Docker configurations align with new structure

## Migration Steps

### Phase 1: Preparation
1. Create backup of current project state
   - ✅ Completed: Created `refactor/layout-cleanup` branch as backup
2. Document all environment variables and create `.env.example`
   - ✅ Completed: Created `.env.example` with all necessary environment variables
   - ✅ Variables documented: Database, RabbitMQ, API Keys, Application Settings, Service URLs, and Monitoring
3. Review and consolidate dependency management
   - ✅ Completed: Consolidated all dependencies into `pyproject.toml`
   - ✅ Removed redundant `requirements.txt` and `setup.py`
4. Create new directory structure while preserving existing files
   - ✅ Completed: Created `src/` directory
   - ✅ Completed: Moved `recruitment/` package into `src/`
   - ✅ Completed: Preserved all existing files during restructuring

### Phase 2: Code Migration
1. Move root Python files to appropriate locations:
   - ✅ `streamlit_app.py` → `src/recruitment/services/`
   - ✅ `process_url.py` → `src/recruitment/services/processing/`
   - ✅ `populate_queue.py` → `scripts/`
   - ✅ `test_imports.py` → `tests/unit/`
   - ✅ `test_search.py` → `tests/unit/`

2. Restructure services:
   - ⚠️ Move existing services into new `src/recruitment/services/` structure
     - ✅ Processing service organized in `src/recruitment/services/processing/`
     - ✅ Discovery service organized in `src/recruitment/services/discovery/`
       - ✅ Moved `url_discovery_service.py` to new location
     - ✅ LLM service organized in `src/recruitment/services/llm/`
     - ⚠️ Files still needing to be moved from old `recruitment/` directory:
       - `web_crawler_lib.py` → `src/recruitment/utils/` (✅ Resolved - moved to correct location)
       - `storage.py` → `src/recruitment/db/` (✅ Resolved - moved to correct location)
       - `recruitment_models.py` → `src/recruitment/models/` (✅ Resolved - kept newer version)
       - `recruitment_db.py` → `src/recruitment/db/` (✅ Resolved - kept newer version)
       - `rabbitmq_utils.py` → `src/recruitment/utils/` (✅ Resolved - kept newer version)
       - `prompts.py` → `src/recruitment/prompts/` (✅ Resolved - content already in __init__.py)
       - `process_urls_from_queue.py` → `src/recruitment/workers/` (✅ Resolved - kept newer version)
       - `models.py` → `src/recruitment/models/` (✅ Resolved - functionality covered by newer models)
       - `config_validator.py` → `src/recruitment/config/` (✅ Resolved - moved to correct location)
       - `batch_processor.py` → `src/recruitment/services/processing/` (✅ Resolved - kept newer version)
       - `response_processor_functions.py` → `src/recruitment/services/processing/` (✅ Resolved - kept newer version)
   - ✅ Create necessary `__init__.py`
   - ✅ Set up logging configuration

3. Database and Models:
   - ✅ Create new `db/` directory structure
   - ✅ Move existing database-related code
   - ✅ Set up migrations directory

### Phase 3: Configuration Updates
1. Update `pyproject.toml`:
   - ✅ Consolidate dependencies
   - ✅ Update package configuration
   - ✅ Configure build system

2. Update Docker configurations:
   - ✅ Update Dockerfiles to use pyproject.toml
   - ✅ Update docker-compose.yml

3. Update import statements in all files to reflect new structure:
   - ✅ Review and update all Python files
     - ✅ Updated imports in `src/recruitment/services/processing/main.py`
     - ✅ Updated imports in `src/recruitment/services/discovery/main.py`
     - ✅ Updated imports in `src/recruitment/services/streamlit_app.py`
     - ✅ Updated imports in `src/recruitment/services/llm/llm_service.py`
   - ✅ Verify relative imports work correctly
   - ⚠️ Test imports after updates (pending verification)

### Phase 4: Testing and Validation
1. Reorganize tests:
   - ✅ Move existing tests to appropriate categories
     - ✅ Unit Tests:
       - ✅ `test_url_processing.py` → `tests/unit/services/processing/test_main.py`
       - ✅ `test_url_discovery.py` → `tests/unit/services/discovery/test_main.py`
       - ✅ `test_database.py` → `tests/unit/db/test_repository.py`
       - ✅ `test_llm_service.py` → `tests/unit/services/llm/test_service.py`
     - ✅ Integration Tests:
       - ✅ `test_process_urls_from_queue.py` → `tests/integration/workers/test_queue_processor.py`
   - ✅ Update test imports
     - ✅ Updated imports in all test files to use correct relative paths
     - ✅ Updated imports to reference new module locations
     - ✅ Fixed Python path resolution in all test files
   - ⚠️ Verify test coverage (pending test execution)

2. Create new test structure:
   - ✅ Set up unit test directory
     - ✅ Created `tests/unit/services/` with subdirectories for each service
     - ✅ Created `tests/unit/db/` for database tests
   - ✅ Set up integration test directory
     - ✅ Created `tests/integration/workers/` for worker integration tests
   - ⚠️ Set up e2e test directory (pending e2e test creation)

### Phase 5: Documentation and Cleanup
1. Update README.md with new structure:
   - ✅ Document new directory layout
     - ✅ Updated project structure diagram
     - ✅ Added detailed directory descriptions
     - ✅ Included new components (db/, workers/, utils/, prompts/)
   - ✅ Update installation instructions
     - ✅ Simplified virtual environment setup
     - ✅ Updated dependency installation steps
     - ✅ Added Windows-specific commands
   - ✅ Update development guidelines
     - ✅ Enhanced test running instructions
     - ✅ Added specific test file examples
     - ✅ Maintained existing deployment and monitoring sections
   - ✅ Preserved existing documentation
     - ✅ URL Processing Dashboard
     - ✅ Multiple Jobs Per URL Support
     - ✅ Database Structure
     - ✅ Known Issues and Solutions

2. Remove obsolete files and directories:
   - ⚠️ Clean up old configuration files
   - ⚠️ Remove redundant directories
   - ⚠️ Archive old test files

3. Update .gitignore:
   - ⚠️ Add new patterns for build artifacts
   - ⚠️ Update database file patterns
   - ⚠️ Add IDE-specific patterns

4. Verify all paths in configuration files:
   - ⚠️ Check Docker configurations
   - ⚠️ Verify test paths
   - ⚠️ Validate import paths

## Risk Mitigation

1. **Backup Strategy**
   - ✅ Create git branch before starting migration
   - ⚠️ Keep original files until new structure is verified
   - ⚠️ Document all changes in commits

2. **Testing Strategy**
   - ⚠️ Run tests after each major move
   - ⚠️ Verify imports work in new structure
   - ⚠️ Check Docker builds

3. **Rollback Plan**
   - ✅ Keep original structure in separate branch
   - ⚠️ Document all changes for potential rollback
   - ⚠️ Test rollback procedure

## Success Criteria

1. All code moved to new structure
2. All tests passing
3. Docker builds successfully
4. All imports working correctly
5. Documentation updated
6. No regression in functionality

## Next Steps

1. ✅ Review this plan and provide feedback
2. ✅ Create backup branch
3. ✅ Begin with Phase 1 preparation
4. ⚠️ Execute migration in small, testable steps
5. ⚠️ Verify each step before proceeding

# Project Simplification TODO

## Architecture Simplification
- [ ] Consolidate `url_discovery` and `url_processing` into a single service
- [ ] Remove RabbitMQ dependency and implement direct processing
- [ ] Simplify deployment by reducing to a single container
- [ ] Remove unnecessary service boundaries and API endpoints

## Database Simplification
- [ ] Reduce database schema to essential tables only:
  - [ ] urls (for discovered URLs)
  - [ ] content (for processed content)
  - [ ] metadata (for basic job/company info)
- [ ] Remove excessive normalization
- [ ] Eliminate unnecessary many-to-many relationships
- [ ] Simplify table structure to match actual use cases

## Code Structure Cleanup
- [ ] Remove redundant service classes:
  - [ ] `URLDiscoveryService`
  - [ ] `URLProcessingService`
  - [ ] `RecruitmentAdSearch`
- [ ] Eliminate unnecessary abstraction layers
- [ ] Remove duplicate functionality across services
- [ ] Consolidate similar functions and methods

## Error Handling Simplification
- [ ] Remove custom exception classes
- [ ] Simplify error handling patterns
- [ ] Reduce logging verbosity
- [ ] Implement straightforward error reporting

## Configuration Simplification
- [ ] Reduce configuration options to essential parameters
- [ ] Consolidate environment variables
- [ ] Remove unused configuration options
- [ ] Simplify configuration management

## Performance Optimization
- [ ] Remove unnecessary database queries
- [ ] Optimize data processing pipeline
- [ ] Reduce memory footprint
- [ ] Simplify resource usage

## Documentation Updates
- [ ] Update README to reflect simplified architecture
- [ ] Document new streamlined workflow
- [ ] Update deployment instructions
- [ ] Add clear examples of basic usage

## Testing
- [ ] Simplify test suite to match new architecture
- [ ] Remove redundant test cases
- [ ] Focus on essential functionality testing
- [ ] Update integration tests for new structure

## Future Considerations
- [ ] Monitor system performance after simplification
- [ ] Gather metrics on resource usage
- [ ] Document any new requirements that emerge
- [ ] Plan for gradual feature additions if needed