-- =============================================================================
-- sync_task.sql
-- Snowflake-native tag change detection
--
-- This does NOT replace tag_sync.py — it complements it.
-- Purpose: detect when column tags change inside Snowflake and write a
-- change log to TAG_CHANGE_LOG. tag_sync.py can then be triggered
-- externally (cron, CI, webhook) to push those changes to Neo4j.
--
-- Setup order:
--   1. Run this file in Snowflake to create the infrastructure
--   2. Schedule tag_sync.py externally (see below)
--
-- Scheduling tag_sync.py externally:
--
--   Cron (Linux/Mac):
--     0 * * * * cd /path/to/repo && python snowflake-neo4j-security/tag_sync.py >> logs/tag_sync.log 2>&1
--
--   Windows Task Scheduler:
--     Program : python
--     Args    : C:\path\to\repo\snowflake-neo4j-security\tag_sync.py
--     Schedule: Hourly
--
--   GitHub Actions (add to .github/workflows/tag_sync.yml):
--     on:
--       schedule:
--         - cron: '0 * * * *'
--     jobs:
--       sync:
--         runs-on: ubuntu-latest
--         steps:
--           - uses: actions/checkout@v4
--           - run: pip install -r requirements.txt
--           - run: python snowflake-neo4j-security/tag_sync.py
--             env:
--               SNOWFLAKE_ACCOUNT:  ${{ secrets.SNOWFLAKE_ACCOUNT }}
--               SNOWFLAKE_USER:     ${{ secrets.SNOWFLAKE_USER }}
--               SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
--               NEO4J_URI:          ${{ secrets.NEO4J_URI }}
--               NEO4J_USER:         ${{ secrets.NEO4J_USER }}
--               NEO4J_PASSWORD:     ${{ secrets.NEO4J_PASSWORD }}
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA   DEMO_SCHEMA;
USE ROLE     SYSADMIN;
USE WAREHOUSE COMPUTE_WH;


-- =============================================================================
-- 1. TAG SNAPSHOT TABLE
--    Stores the last-known tag state for every column.
--    The Task refreshes this table on each run and writes diffs to TAG_CHANGE_LOG.
-- =============================================================================

CREATE TABLE IF NOT EXISTS TAG_SNAPSHOT (
    snapshot_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    table_name         VARCHAR,
    column_name        VARCHAR,
    data_classification VARCHAR,
    data_category      VARCHAR,
    encryption_required VARCHAR,
    retention_policy   VARCHAR,
    data_owner         VARCHAR,
    captured_at        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_name, column_name)
);


-- =============================================================================
-- 2. TAG CHANGE LOG TABLE
--    Each row records one field that changed on a column.
--    tag_sync.py queries this table to confirm what has changed before writing
--    to Neo4j (optional — tag_sync.py does its own diff independently).
-- =============================================================================

CREATE TABLE IF NOT EXISTS TAG_CHANGE_LOG (
    change_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    changed_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    table_name         VARCHAR,
    column_name        VARCHAR,
    tag_field          VARCHAR,       -- e.g. 'DATA_CLASSIFICATION'
    old_value          VARCHAR,
    new_value          VARCHAR,
    sync_status        VARCHAR DEFAULT 'PENDING',  -- PENDING | SYNCED
    synced_at          TIMESTAMP_TZ
);


-- =============================================================================
-- 3. STORED PROCEDURE: DETECT_TAG_CHANGES
--    Compares current TAG_REFERENCES_ALL_COLUMNS against TAG_SNAPSHOT.
--    Writes new/changed rows to TAG_CHANGE_LOG, then refreshes TAG_SNAPSHOT.
-- =============================================================================

CREATE OR REPLACE PROCEDURE DETECT_TAG_CHANGES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    changed_count INTEGER DEFAULT 0;
BEGIN

    -- Step 1: Build current tag state from all tables
    CREATE OR REPLACE TEMPORARY TABLE CURRENT_TAGS AS
    SELECT
        t.object_name                                                            AS table_name,
        t.column_name,
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
    GROUP BY t.object_name, t.column_name;


    -- Step 2: Write changes vs snapshot to TAG_CHANGE_LOG
    --         (new columns OR any tag field that differs from last snapshot)
    INSERT INTO TAG_CHANGE_LOG (table_name, column_name, tag_field, old_value, new_value)
    SELECT
        ct.table_name,
        ct.column_name,
        changes.tag_field,
        changes.old_value,
        changes.new_value
    FROM CURRENT_TAGS ct
    LEFT JOIN TAG_SNAPSHOT snap
        ON  snap.table_name   = ct.table_name
        AND snap.column_name  = ct.column_name
    CROSS JOIN LATERAL (
        SELECT 'DATA_CLASSIFICATION' AS tag_field,
               snap.data_classification AS old_value, ct.data_classification AS new_value
        WHERE  COALESCE(snap.data_classification, '') <> COALESCE(ct.data_classification, '')
        UNION ALL
        SELECT 'DATA_CATEGORY', snap.data_category, ct.data_category
        WHERE  COALESCE(snap.data_category, '') <> COALESCE(ct.data_category, '')
        UNION ALL
        SELECT 'ENCRYPTION_REQUIRED', snap.encryption_required, ct.encryption_required
        WHERE  COALESCE(snap.encryption_required, '') <> COALESCE(ct.encryption_required, '')
        UNION ALL
        SELECT 'RETENTION_POLICY', snap.retention_policy, ct.retention_policy
        WHERE  COALESCE(snap.retention_policy, '') <> COALESCE(ct.retention_policy, '')
        UNION ALL
        SELECT 'DATA_OWNER', snap.data_owner, ct.data_owner
        WHERE  COALESCE(snap.data_owner, '') <> COALESCE(ct.data_owner, '')
    ) changes
    WHERE snap.column_name IS NULL  -- new column
       OR changes.tag_field IS NOT NULL;  -- existing column with a changed tag

    changed_count := SQLROWCOUNT;


    -- Step 3: Refresh TAG_SNAPSHOT with current state
    MERGE INTO TAG_SNAPSHOT AS snap
    USING CURRENT_TAGS AS ct
        ON  snap.table_name  = ct.table_name
        AND snap.column_name = ct.column_name
    WHEN MATCHED THEN UPDATE SET
        data_classification = ct.data_classification,
        data_category       = ct.data_category,
        encryption_required = ct.encryption_required,
        retention_policy    = ct.retention_policy,
        data_owner          = ct.data_owner,
        captured_at         = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT
        (table_name, column_name, data_classification, data_category,
         encryption_required, retention_policy, data_owner)
    VALUES
        (ct.table_name, ct.column_name, ct.data_classification, ct.data_category,
         ct.encryption_required, ct.retention_policy, ct.data_owner);

    RETURN 'OK: ' || changed_count || ' tag changes logged.';
END;
$$;


-- =============================================================================
-- 4. SNOWFLAKE TASK: Run DETECT_TAG_CHANGES every hour
--    The task only records drift in Snowflake.
--    tag_sync.py (scheduled separately) reads this data and writes to Neo4j.
-- =============================================================================

CREATE OR REPLACE TASK SYNC_TAGS_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 * * * * UTC'   -- every hour on the hour
    COMMENT   = 'Detect Snowflake tag changes and write to TAG_CHANGE_LOG'
AS
    CALL DETECT_TAG_CHANGES();

-- Activate the task (tasks start suspended by default)
ALTER TASK SYNC_TAGS_TASK RESUME;


-- =============================================================================
-- 5. HELPER QUERIES
-- =============================================================================

-- View pending (not yet synced to Neo4j) tag changes:
SELECT
    changed_at,
    table_name,
    column_name,
    tag_field,
    old_value,
    new_value
FROM TAG_CHANGE_LOG
WHERE sync_status = 'PENDING'
ORDER BY changed_at DESC;

-- Mark all pending changes as synced (call this after tag_sync.py completes):
-- UPDATE TAG_CHANGE_LOG
-- SET sync_status = 'SYNCED', synced_at = CURRENT_TIMESTAMP
-- WHERE sync_status = 'PENDING';

-- View full change history for a specific column:
-- SELECT * FROM TAG_CHANGE_LOG
-- WHERE table_name = 'EMPLOYEES' AND column_name = 'salary'
-- ORDER BY changed_at DESC;

-- Manually trigger the procedure without waiting for the Task schedule:
-- CALL DETECT_TAG_CHANGES();

-- Pause / resume the task:
-- ALTER TASK SYNC_TAGS_TASK SUSPEND;
-- ALTER TASK SYNC_TAGS_TASK RESUME;

-- Check task run history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP),
--     TASK_NAME => 'SYNC_TAGS_TASK'
-- ));
