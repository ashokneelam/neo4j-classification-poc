-- =============================================================================
-- LIVE DEMO: Snowflake Side
-- Snowflake -> Neo4j Data Classification & Security POC
--
-- Run these queries in Snowflake Worksheets (in order).
-- Each section number matches demo_neo4j.cypher for side-by-side comparison.
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA   DEMO_SCHEMA;
USE ROLE     SYSADMIN;
USE WAREHOUSE COMPUTE_WH;


-- =============================================================================
-- SECTION 1: THE SCHEMA — What tables exist and how are they structured?
-- =============================================================================

-- 1a. All tables in the demo schema
SELECT table_name, row_count, bytes, created
FROM   information_schema.tables
WHERE  table_schema = 'DEMO_SCHEMA'
ORDER  BY table_name;

-- 1b. Full column inventory across all 5 tables
SELECT table_name, column_name, data_type, is_nullable, ordinal_position
FROM   information_schema.columns
WHERE  table_schema = 'DEMO_SCHEMA'
ORDER  BY table_name, ordinal_position;


-- =============================================================================
-- SECTION 2: TAGS IN SNOWFLAKE — Column-level classification metadata
-- =============================================================================

-- 2a. All tags defined in the schema (the taxonomy)
SHOW TAGS IN SCHEMA DEMO_SCHEMA;

-- 2b. Every column tag across all 5 tables (raw tag reference view)
--     This is exactly what the pipeline reads to build Neo4j Column nodes
SELECT
    object_name   AS table_name,
    column_name,
    tag_name,
    tag_value
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
UNION ALL
SELECT object_name, column_name, tag_name, tag_value
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
UNION ALL
SELECT object_name, column_name, tag_name, tag_value
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
UNION ALL
SELECT object_name, column_name, tag_name, tag_value
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'))
UNION ALL
SELECT object_name, column_name, tag_name, tag_value
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'))
ORDER BY table_name, column_name, tag_name;

-- 2c. Pivot: one row per column with all tags as columns
--     (This is what the pipeline ETLs into Neo4j :Column nodes)
SELECT
    t.object_name                                               AS table_name,
    t.column_name,
    c.data_type,
    c.ordinal_position,
    MAX(CASE WHEN t.tag_name = 'DATA_CLASSIFICATION' THEN t.tag_value END)  AS data_classification,
    MAX(CASE WHEN t.tag_name = 'DATA_CATEGORY'       THEN t.tag_value END)  AS data_category,
    MAX(CASE WHEN t.tag_name = 'ENCRYPTION_REQUIRED' THEN t.tag_value END)  AS encryption_required,
    MAX(CASE WHEN t.tag_name = 'RETENTION_POLICY'    THEN t.tag_value END)  AS retention_policy,
    MAX(CASE WHEN t.tag_name = 'DATA_OWNER'          THEN t.tag_value END)  AS data_owner
FROM (
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'))
) t
JOIN information_schema.columns c
    ON c.table_name   = t.object_name
    AND c.column_name  = t.column_name
    AND c.table_schema = 'DEMO_SCHEMA'
GROUP BY t.object_name, t.column_name, c.data_type, c.ordinal_position
ORDER BY t.object_name, c.ordinal_position;


-- =============================================================================
-- SECTION 3: TAG COVERAGE SUMMARY
-- =============================================================================

-- 3a. How many columns per classification tier?
SELECT
    tag_value   AS classification,
    COUNT(*)    AS column_count
FROM (
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'))
) all_tags
WHERE tag_name = 'DATA_CLASSIFICATION'
GROUP BY tag_value
ORDER BY column_count DESC;

-- 3b. Which columns require encryption?
SELECT
    t.object_name   AS table_name,
    t.column_name,
    MAX(CASE WHEN t.tag_name = 'DATA_CLASSIFICATION' THEN t.tag_value END) AS classification,
    MAX(CASE WHEN t.tag_name = 'DATA_OWNER'          THEN t.tag_value END) AS data_owner
FROM (
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name
    FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
) t
WHERE t.tag_name = 'ENCRYPTION_REQUIRED' AND t.tag_value = 'Yes'
GROUP BY t.object_name, t.column_name
ORDER BY t.object_name, t.column_name;


-- =============================================================================
-- SECTION 4: MASKING POLICIES — Column-level security in Snowflake
-- =============================================================================

-- 4a. What masking policies exist?
SHOW MASKING POLICIES IN SCHEMA DEMO_SCHEMA;

-- 4b. What row access policies exist?
SHOW ROW ACCESS POLICIES IN SCHEMA DEMO_SCHEMA;

-- 4c. Which columns have masking policies applied?
SELECT
    ref_entity_name   AS table_name,
    ref_column_name   AS column_name,
    policy_name,
    policy_kind
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name   => 'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS'
))
UNION ALL
SELECT ref_entity_name, ref_column_name, policy_name, policy_kind
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name   => 'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES'
))
UNION ALL
SELECT ref_entity_name, ref_column_name, policy_name, policy_kind
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name   => 'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS'
))
UNION ALL
SELECT ref_entity_name, ref_column_name, policy_name, policy_kind
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name   => 'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS'
))
ORDER BY table_name, column_name;

-- 4d. MASKING IN ACTION — run as DATA_ENGINEER (sees everything)
USE ROLE DATA_ENGINEER;
SELECT employee_id, first_name, last_name, email, ssn, salary
FROM   employees
LIMIT  5;

-- 4e. MASKING IN ACTION — same query as HR_MANAGER (sees PII, SSN last 4, salary masked)
USE ROLE HR_MANAGER;
SELECT employee_id, first_name, last_name, email, ssn, salary
FROM   employees
LIMIT  5;

-- 4f. MASKING IN ACTION — same query as DATA_ANALYST (email domain only, SSN/salary fully masked)
USE ROLE DATA_ANALYST;
SELECT employee_id, first_name, last_name, email, ssn, salary
FROM   employees
LIMIT  5;

-- Reset back to SYSADMIN after demos
USE ROLE SYSADMIN;


-- =============================================================================
-- SECTION 5: THE ETL BRIDGE — What the pipeline reads to build the graph
-- =============================================================================

-- 5a. This is the EXACT query the Python pipeline executes to extract all tags
--     (shown here for transparency — see pipeline.py extract_column_metadata())
SELECT
    t.object_name                                               AS table_name,
    t.column_name,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.is_nullable,
    c.ordinal_position,
    MAX(CASE WHEN t.tag_name = 'DATA_CLASSIFICATION' THEN t.tag_value END)  AS data_classification,
    MAX(CASE WHEN t.tag_name = 'DATA_CATEGORY'       THEN t.tag_value END)  AS data_category,
    MAX(CASE WHEN t.tag_name = 'ENCRYPTION_REQUIRED' THEN t.tag_value END)  AS encryption_required,
    MAX(CASE WHEN t.tag_name = 'RETENTION_POLICY'    THEN t.tag_value END)  AS retention_policy,
    MAX(CASE WHEN t.tag_name = 'DATA_OWNER'          THEN t.tag_value END)  AS data_owner
FROM (
    SELECT tag_name, tag_value, object_name, column_name FROM TABLE(
        SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
            'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name FROM TABLE(
        SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
            'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name FROM TABLE(
        SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
            'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name FROM TABLE(
        SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
            'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value, object_name, column_name FROM TABLE(
        SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
            'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'))
) t
JOIN information_schema.columns c
    ON c.table_name   = t.object_name
    AND c.column_name  = t.column_name
    AND c.table_schema = 'DEMO_SCHEMA'
GROUP BY t.object_name, t.column_name, c.data_type, c.character_maximum_length,
         c.numeric_precision, c.is_nullable, c.ordinal_position
ORDER BY t.object_name, c.ordinal_position;
