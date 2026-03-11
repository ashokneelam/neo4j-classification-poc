-- =============================================================================
-- STEP 1: SNOWFLAKE TAG SETUP
-- Project: Snowflake → Neo4j Data Classification & Security Demo
-- =============================================================================

USE ROLE SYSADMIN;

-- Create a dedicated database and schema for the demo
CREATE DATABASE IF NOT EXISTS SECURITY_DEMO_DB;
USE DATABASE SECURITY_DEMO_DB;

CREATE SCHEMA IF NOT EXISTS DEMO_SCHEMA;
USE SCHEMA DEMO_SCHEMA;

-- =============================================================================
-- CREATE CLASSIFICATION TAGS
-- These tags will be propagated to Neo4j as node properties
-- =============================================================================

-- Data Classification Tag (PII, Restricted, Internal, Public)
CREATE OR REPLACE TAG data_classification
    ALLOWED_VALUES 'PII', 'Restricted', 'Internal', 'Public'
    COMMENT = 'Classifies data sensitivity level for governance and access control';

-- Data Category Tag (for more granular categorization)
CREATE OR REPLACE TAG data_category
    ALLOWED_VALUES 'Personal', 'Financial', 'Operational', 'Marketing', 'System'
    COMMENT = 'Categorizes data by business domain';

-- Encryption Required Tag
CREATE OR REPLACE TAG encryption_required
    ALLOWED_VALUES 'Yes', 'No'
    COMMENT = 'Indicates if encryption is required for this column';

-- Retention Policy Tag
CREATE OR REPLACE TAG retention_policy
    ALLOWED_VALUES '30_days', '1_year', '7_years', 'Indefinite'
    COMMENT = 'Data retention policy for compliance';

-- Owner Tag
CREATE OR REPLACE TAG data_owner
    COMMENT = 'Business owner responsible for this data element';

SHOW TAGS IN SCHEMA DEMO_SCHEMA;
