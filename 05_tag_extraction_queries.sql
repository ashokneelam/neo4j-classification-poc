-- =============================================================================
-- STEP 5: TAG EXTRACTION QUERIES FOR ETL TO NEO4J
-- Project: Snowflake → Neo4j Data Classification & Security Demo
-- These queries are used by the Python ingestion pipeline
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA DEMO_SCHEMA;
USE ROLE SYSADMIN;

-- =============================================================================
-- QUERY 1: Extract ALL column-level tag metadata (used by ETL pipeline)
-- =============================================================================
SELECT 
    tag_database,
    tag_schema,
    tag_name,
    tag_value,
    object_database,
    object_schema,
    object_name,
    column_name,
    domain
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'
))
UNION ALL
SELECT 
    tag_database, tag_schema, tag_name, tag_value,
    object_database, object_schema, object_name, column_name, domain
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'
))
UNION ALL
SELECT 
    tag_database, tag_schema, tag_name, tag_value,
    object_database, object_schema, object_name, column_name, domain
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'
))
UNION ALL
SELECT 
    tag_database, tag_schema, tag_name, tag_value,
    object_database, object_schema, object_name, column_name, domain
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'
))
UNION ALL
SELECT 
    tag_database, tag_schema, tag_name, tag_value,
    object_database, object_schema, object_name, column_name, domain
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'
))
ORDER BY object_name, column_name, tag_name;


-- =============================================================================
-- QUERY 2: Column metadata with data types + tags (FULL SCHEMA SNAPSHOT)
-- Used to build Neo4j :Column nodes with all metadata
-- =============================================================================
SELECT 
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT,
    c.ORDINAL_POSITION,
    -- Tag values via subqueries (alternatively use tag_references view)
    SYSTEM$GET_TAG('SECURITY_DEMO_DB.DEMO_SCHEMA.DATA_CLASSIFICATION', 
                   c.TABLE_CATALOG || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME, 
                   'COLUMN')       AS data_classification,
    SYSTEM$GET_TAG('SECURITY_DEMO_DB.DEMO_SCHEMA.DATA_CATEGORY', 
                   c.TABLE_CATALOG || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME, 
                   'COLUMN')       AS data_category,
    SYSTEM$GET_TAG('SECURITY_DEMO_DB.DEMO_SCHEMA.ENCRYPTION_REQUIRED', 
                   c.TABLE_CATALOG || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME, 
                   'COLUMN')       AS encryption_required,
    SYSTEM$GET_TAG('SECURITY_DEMO_DB.DEMO_SCHEMA.RETENTION_POLICY', 
                   c.TABLE_CATALOG || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME, 
                   'COLUMN')       AS retention_policy,
    SYSTEM$GET_TAG('SECURITY_DEMO_DB.DEMO_SCHEMA.DATA_OWNER', 
                   c.TABLE_CATALOG || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '.' || c.COLUMN_NAME, 
                   'COLUMN')       AS data_owner
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'DEMO_SCHEMA'
  AND c.TABLE_CATALOG = 'SECURITY_DEMO_DB'
  AND c.TABLE_NAME IN ('CUSTOMERS', 'FINANCIAL_TRANSACTIONS', 'EMPLOYEES', 'PRODUCTS', 'AUDIT_LOGS')
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;


-- =============================================================================
-- QUERY 3: Masking policy references (for Neo4j policy nodes)
-- =============================================================================
SELECT 
    ref_entity_name         AS table_name,
    ref_column_name         AS column_name,
    policy_name,
    policy_kind,
    policy_status
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name => 'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS'
))
UNION ALL
SELECT ref_entity_name, ref_column_name, policy_name, policy_kind, policy_status
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name => 'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS'
))
UNION ALL
SELECT ref_entity_name, ref_column_name, policy_name, policy_kind, policy_status
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name => 'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES'
))
ORDER BY table_name, column_name;


-- =============================================================================
-- QUERY 4: Role-level grants (for Neo4j role nodes)
-- =============================================================================
SHOW GRANTS TO ROLE DATA_ENGINEER;
SHOW GRANTS TO ROLE DATA_ANALYST;
SHOW GRANTS TO ROLE HR_MANAGER;
SHOW GRANTS TO ROLE FINANCE_ANALYST;
SHOW GRANTS TO ROLE SECURITY_AUDITOR;
SHOW GRANTS TO ROLE PUBLIC_USER;
