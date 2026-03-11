-- =============================================================================
-- STEP 3: APPLY TAGS TO COLUMNS
-- Project: Snowflake → Neo4j Data Classification & Security Demo
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA DEMO_SCHEMA;
USE ROLE SYSADMIN;

-- =============================================================================
-- CUSTOMERS TABLE - Column-Level Tags
-- =============================================================================

-- PII columns
ALTER TABLE customers MODIFY COLUMN first_name      SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN last_name       SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN email           SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN phone           SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN date_of_birth   SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN address_line1   SET TAG data_classification = 'PII';
ALTER TABLE customers MODIFY COLUMN address_line2   SET TAG data_classification = 'PII';

-- Restricted columns
ALTER TABLE customers MODIFY COLUMN ssn             SET TAG data_classification = 'Restricted';

-- Internal columns
ALTER TABLE customers MODIFY COLUMN customer_id     SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN city            SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN state           SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN zip_code        SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN customer_tier   SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN created_at      SET TAG data_classification = 'Internal';
ALTER TABLE customers MODIFY COLUMN is_active       SET TAG data_classification = 'Internal';

-- Public columns
ALTER TABLE customers MODIFY COLUMN country         SET TAG data_classification = 'Public';

-- Additional tags for customers
ALTER TABLE customers MODIFY COLUMN email           SET TAG data_category = 'Personal';
ALTER TABLE customers MODIFY COLUMN email           SET TAG encryption_required = 'Yes';
ALTER TABLE customers MODIFY COLUMN ssn             SET TAG encryption_required = 'Yes';
ALTER TABLE customers MODIFY COLUMN ssn             SET TAG retention_policy = '7_years';
ALTER TABLE customers MODIFY COLUMN date_of_birth   SET TAG encryption_required = 'Yes';
ALTER TABLE customers MODIFY COLUMN email           SET TAG data_owner = 'Customer Success Team';
ALTER TABLE customers MODIFY COLUMN ssn             SET TAG data_owner = 'Compliance Team';


-- =============================================================================
-- FINANCIAL_TRANSACTIONS TABLE - Column-Level Tags
-- =============================================================================

-- Restricted columns
ALTER TABLE financial_transactions MODIFY COLUMN account_number      SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN routing_number      SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN card_last_four      SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN transaction_amount  SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN fraud_flag          SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN ip_address          SET TAG data_classification = 'Restricted';
ALTER TABLE financial_transactions MODIFY COLUMN device_fingerprint  SET TAG data_classification = 'Restricted';

-- Internal columns
ALTER TABLE financial_transactions MODIFY COLUMN transaction_id      SET TAG data_classification = 'Internal';
ALTER TABLE financial_transactions MODIFY COLUMN customer_id         SET TAG data_classification = 'Internal';
ALTER TABLE financial_transactions MODIFY COLUMN transaction_type    SET TAG data_classification = 'Internal';
ALTER TABLE financial_transactions MODIFY COLUMN merchant_name       SET TAG data_classification = 'Internal';
ALTER TABLE financial_transactions MODIFY COLUMN transaction_date    SET TAG data_classification = 'Internal';
ALTER TABLE financial_transactions MODIFY COLUMN status              SET TAG data_classification = 'Internal';

-- Public columns
ALTER TABLE financial_transactions MODIFY COLUMN merchant_category   SET TAG data_classification = 'Public';
ALTER TABLE financial_transactions MODIFY COLUMN currency            SET TAG data_classification = 'Public';

-- Additional tags
ALTER TABLE financial_transactions MODIFY COLUMN account_number      SET TAG encryption_required = 'Yes';
ALTER TABLE financial_transactions MODIFY COLUMN account_number      SET TAG retention_policy = '7_years';
ALTER TABLE financial_transactions MODIFY COLUMN transaction_amount  SET TAG data_category = 'Financial';
ALTER TABLE financial_transactions MODIFY COLUMN fraud_flag          SET TAG data_owner = 'Risk & Fraud Team';
ALTER TABLE financial_transactions MODIFY COLUMN ip_address          SET TAG data_category = 'Personal';


-- =============================================================================
-- EMPLOYEES TABLE - Column-Level Tags
-- =============================================================================

-- PII columns
ALTER TABLE employees MODIFY COLUMN first_name          SET TAG data_classification = 'PII';
ALTER TABLE employees MODIFY COLUMN last_name           SET TAG data_classification = 'PII';
ALTER TABLE employees MODIFY COLUMN email               SET TAG data_classification = 'PII';
ALTER TABLE employees MODIFY COLUMN personal_email      SET TAG data_classification = 'PII';
ALTER TABLE employees MODIFY COLUMN phone               SET TAG data_classification = 'PII';
ALTER TABLE employees MODIFY COLUMN date_of_birth       SET TAG data_classification = 'PII';

-- Restricted columns
ALTER TABLE employees MODIFY COLUMN ssn                 SET TAG data_classification = 'Restricted';
ALTER TABLE employees MODIFY COLUMN salary              SET TAG data_classification = 'Restricted';
ALTER TABLE employees MODIFY COLUMN bonus               SET TAG data_classification = 'Restricted';
ALTER TABLE employees MODIFY COLUMN bank_account        SET TAG data_classification = 'Restricted';
ALTER TABLE employees MODIFY COLUMN clearance_level     SET TAG data_classification = 'Restricted';

-- Internal columns
ALTER TABLE employees MODIFY COLUMN employee_id         SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN employee_number     SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN hire_date           SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN termination_date    SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN department          SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN job_title           SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN manager_id          SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN performance_rating  SET TAG data_classification = 'Internal';
ALTER TABLE employees MODIFY COLUMN remote_work         SET TAG data_classification = 'Internal';

-- Public columns
ALTER TABLE employees MODIFY COLUMN location_office     SET TAG data_classification = 'Public';

-- Additional tags
ALTER TABLE employees MODIFY COLUMN ssn                 SET TAG encryption_required = 'Yes';
ALTER TABLE employees MODIFY COLUMN salary              SET TAG encryption_required = 'Yes';
ALTER TABLE employees MODIFY COLUMN bank_account        SET TAG encryption_required = 'Yes';
ALTER TABLE employees MODIFY COLUMN ssn                 SET TAG retention_policy = '7_years';
ALTER TABLE employees MODIFY COLUMN salary              SET TAG retention_policy = '7_years';
ALTER TABLE employees MODIFY COLUMN ssn                 SET TAG data_owner = 'HR Team';
ALTER TABLE employees MODIFY COLUMN salary              SET TAG data_owner = 'Finance Team';
ALTER TABLE employees MODIFY COLUMN clearance_level     SET TAG data_owner = 'Security Team';


-- =============================================================================
-- PRODUCTS TABLE - Column-Level Tags
-- =============================================================================

-- Public columns
ALTER TABLE products MODIFY COLUMN product_sku          SET TAG data_classification = 'Public';
ALTER TABLE products MODIFY COLUMN product_name         SET TAG data_classification = 'Public';
ALTER TABLE products MODIFY COLUMN product_description  SET TAG data_classification = 'Public';
ALTER TABLE products MODIFY COLUMN category             SET TAG data_classification = 'Public';
ALTER TABLE products MODIFY COLUMN subcategory          SET TAG data_classification = 'Public';
ALTER TABLE products MODIFY COLUMN launch_date          SET TAG data_classification = 'Public';

-- Restricted columns (pricing/margins are trade secrets)
ALTER TABLE products MODIFY COLUMN cost_price           SET TAG data_classification = 'Restricted';
ALTER TABLE products MODIFY COLUMN profit_margin        SET TAG data_classification = 'Restricted';

-- Internal columns
ALTER TABLE products MODIFY COLUMN product_id           SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN unit_price           SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN stock_quantity       SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN reorder_threshold    SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN supplier_id          SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN is_active            SET TAG data_classification = 'Internal';
ALTER TABLE products MODIFY COLUMN last_modified        SET TAG data_classification = 'Internal';

-- Additional tags
ALTER TABLE products MODIFY COLUMN cost_price           SET TAG data_owner = 'Finance Team';
ALTER TABLE products MODIFY COLUMN profit_margin        SET TAG data_owner = 'Finance Team';
ALTER TABLE products MODIFY COLUMN cost_price           SET TAG data_category = 'Financial';
ALTER TABLE products MODIFY COLUMN profit_margin        SET TAG data_category = 'Financial';


-- =============================================================================
-- AUDIT_LOGS TABLE - Column-Level Tags
-- =============================================================================

-- PII columns
ALTER TABLE audit_logs MODIFY COLUMN user_email         SET TAG data_classification = 'PII';

-- Restricted columns
ALTER TABLE audit_logs MODIFY COLUMN source_ip          SET TAG data_classification = 'Restricted';
ALTER TABLE audit_logs MODIFY COLUMN session_id         SET TAG data_classification = 'Restricted';
ALTER TABLE audit_logs MODIFY COLUMN user_agent         SET TAG data_classification = 'Restricted';

-- Internal columns
ALTER TABLE audit_logs MODIFY COLUMN log_id             SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN event_timestamp    SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN user_id            SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN action_type        SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN resource_accessed  SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN data_classification SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN success_flag       SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN failure_reason     SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN records_accessed   SET TAG data_classification = 'Internal';
ALTER TABLE audit_logs MODIFY COLUMN query_hash         SET TAG data_classification = 'Internal';

-- Additional tags
ALTER TABLE audit_logs MODIFY COLUMN source_ip          SET TAG retention_policy = '1_year';
ALTER TABLE audit_logs MODIFY COLUMN session_id         SET TAG retention_policy = '1_year';
ALTER TABLE audit_logs MODIFY COLUMN source_ip          SET TAG data_owner = 'Security Team';


-- =============================================================================
-- VERIFY TAGS APPLIED CORRECTLY
-- =============================================================================

-- View all column tags in the schema
SELECT 
    object_name         AS table_name,
    column_name,
    tag_name,
    tag_value,
    domain              AS object_type
FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'
))
ORDER BY column_name, tag_name;

-- Summary count by classification
-- Aggregate classification counts across all tagged tables
SELECT 
    tag_value           AS classification,
    COUNT(*)            AS column_count
FROM (
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
    UNION ALL
    SELECT tag_name, tag_value FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
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
