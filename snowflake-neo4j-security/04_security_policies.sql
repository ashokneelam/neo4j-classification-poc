-- =============================================================================
-- STEP 4: SNOWFLAKE ROLES, ROW ACCESS POLICIES & COLUMN MASKING
-- Project: Snowflake → Neo4j Data Classification & Security Demo
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA DEMO_SCHEMA;
USE ROLE SYSADMIN;

-- =============================================================================
-- ROLES HIERARCHY
-- =============================================================================
--
--   ACCOUNTADMIN (Snowflake built-in)
--       └── DATA_GOVERNANCE_ADMIN     (Manages tags, policies)
--               ├── DATA_ENGINEER     (Full read, limited write)
--               ├── DATA_ANALYST      (Read Internal + Public only)
--               ├── HR_MANAGER        (Read PII for employees)
--               ├── FINANCE_ANALYST   (Read Restricted financials)
--               ├── SECURITY_AUDITOR  (Read audit logs + metadata)
--               └── PUBLIC_USER       (Public data only)

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS DATA_GOVERNANCE_ADMIN COMMENT = 'Manages data classification policies and tags';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER         COMMENT = 'Full read access to all classified data';
CREATE ROLE IF NOT EXISTS DATA_ANALYST          COMMENT = 'Read Internal + Public columns only';
CREATE ROLE IF NOT EXISTS HR_MANAGER            COMMENT = 'Read PII for employee data';
CREATE ROLE IF NOT EXISTS FINANCE_ANALYST       COMMENT = 'Read Restricted financial data';
CREATE ROLE IF NOT EXISTS SECURITY_AUDITOR      COMMENT = 'Read audit logs and access metadata';
CREATE ROLE IF NOT EXISTS PUBLIC_USER           COMMENT = 'Read Public data only';

-- Grant role hierarchy
GRANT ROLE DATA_GOVERNANCE_ADMIN    TO ROLE SYSADMIN;
GRANT ROLE DATA_ENGINEER            TO ROLE DATA_GOVERNANCE_ADMIN;
GRANT ROLE DATA_ANALYST             TO ROLE DATA_ENGINEER;
GRANT ROLE HR_MANAGER               TO ROLE DATA_ANALYST;
GRANT ROLE FINANCE_ANALYST          TO ROLE DATA_ANALYST;
GRANT ROLE SECURITY_AUDITOR         TO ROLE DATA_ANALYST;
GRANT ROLE PUBLIC_USER              TO ROLE DATA_ANALYST;

-- Database and schema grants
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE DATA_GOVERNANCE_ADMIN;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE DATA_ANALYST;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE HR_MANAGER;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE FINANCE_ANALYST;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE SECURITY_AUDITOR;
GRANT USAGE ON DATABASE SECURITY_DEMO_DB    TO ROLE PUBLIC_USER;

GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE DATA_GOVERNANCE_ADMIN;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE HR_MANAGER;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE FINANCE_ANALYST;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE SECURITY_AUDITOR;
GRANT USAGE ON SCHEMA DEMO_SCHEMA           TO ROLE PUBLIC_USER;


-- =============================================================================
-- COLUMN-LEVEL MASKING POLICIES
-- Masks sensitive data based on the role querying
-- =============================================================================

-- MASKING POLICY: SSN - Show full for DATA_ENGINEER, partial for HR_MANAGER, masked for others
USE ROLE SYSADMIN;

CREATE OR REPLACE MASKING POLICY ssn_masking_policy AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN')
            THEN val                                         -- Full value
        WHEN CURRENT_ROLE() IN ('HR_MANAGER', 'FINANCE_ANALYST')
            THEN CONCAT('***-**-', RIGHT(val, 4))           -- Show last 4 only
        ELSE '***-**-****'                                  -- Fully masked
    END;

-- MASKING POLICY: Email - Full for privileged, partial domain for analysts
CREATE OR REPLACE MASKING POLICY email_masking_policy AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 'HR_MANAGER')
            THEN val                                         -- Full email
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'SECURITY_AUDITOR')
            THEN CONCAT('***@', SPLIT_PART(val, '@', 2))    -- Domain only
        ELSE '***@***.***'                                  -- Fully masked
    END;

-- MASKING POLICY: Financial amounts - Full for finance, rounded for analysts
CREATE OR REPLACE MASKING POLICY amount_masking_policy AS (val NUMBER) RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 'FINANCE_ANALYST')
            THEN val                                         -- Exact amount
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST')
            THEN ROUND(val, -2)                             -- Rounded to nearest 100
        ELSE -1                                             -- Masked (returns -1 as sentinel)
    END;

-- MASKING POLICY: Account numbers - Show last 4 for finance, masked for others
CREATE OR REPLACE MASKING POLICY account_masking_policy AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN CURRENT_ROLE() IN ('FINANCE_ANALYST')
            THEN CONCAT('****-****-****-', RIGHT(val, 4))
        ELSE '****-****-****-****'
    END;

-- MASKING POLICY: IP Address - Show for security, mask for others
CREATE OR REPLACE MASKING POLICY ip_masking_policy AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 'SECURITY_AUDITOR')
            THEN val
        ELSE '***.***.***.***'
    END;

-- MASKING POLICY: Salary - Full for finance/HR, masked for others
CREATE OR REPLACE MASKING POLICY salary_masking_policy AS (val NUMBER) RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 'FINANCE_ANALYST', 'HR_MANAGER')
            THEN val
        ELSE -1
    END;


-- =============================================================================
-- APPLY MASKING POLICIES TO COLUMNS
-- =============================================================================

ALTER TABLE customers         MODIFY COLUMN ssn            SET MASKING POLICY ssn_masking_policy;
ALTER TABLE customers         MODIFY COLUMN email          SET MASKING POLICY email_masking_policy;
ALTER TABLE employees         MODIFY COLUMN ssn            SET MASKING POLICY ssn_masking_policy;
ALTER TABLE employees         MODIFY COLUMN email          SET MASKING POLICY email_masking_policy;
ALTER TABLE employees         MODIFY COLUMN personal_email SET MASKING POLICY email_masking_policy;
ALTER TABLE employees         MODIFY COLUMN salary         SET MASKING POLICY salary_masking_policy;
ALTER TABLE employees         MODIFY COLUMN bonus          SET MASKING POLICY salary_masking_policy;
ALTER TABLE financial_transactions MODIFY COLUMN account_number   SET MASKING POLICY account_masking_policy;
ALTER TABLE financial_transactions MODIFY COLUMN transaction_amount SET MASKING POLICY amount_masking_policy;
ALTER TABLE financial_transactions MODIFY COLUMN ip_address       SET MASKING POLICY ip_masking_policy;
ALTER TABLE audit_logs        MODIFY COLUMN source_ip      SET MASKING POLICY ip_masking_policy;
ALTER TABLE audit_logs        MODIFY COLUMN user_email     SET MASKING POLICY email_masking_policy;


-- =============================================================================
-- ROW ACCESS POLICIES
-- Restricts which ROWS each role can see
-- =============================================================================

-- Customers: PUBLIC_USER sees no data, DATA_ANALYST sees active customers only
CREATE OR REPLACE ROW ACCESS POLICY customer_row_policy AS (is_active BOOLEAN) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 
                                 'HR_MANAGER', 'FINANCE_ANALYST')
            THEN TRUE                                       -- See all rows
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'SECURITY_AUDITOR')
            THEN is_active = TRUE                           -- Active customers only
        ELSE FALSE                                          -- No access
    END;

-- Employees: HR sees all, others see only non-terminated
CREATE OR REPLACE ROW ACCESS POLICY employee_row_policy AS (termination_date DATE) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 'HR_MANAGER')
            THEN TRUE
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'FINANCE_ANALYST')
            THEN termination_date IS NULL                   -- Active employees only
        ELSE FALSE
    END;

-- Financial Transactions: Only privileged roles see fraud-flagged transactions
CREATE OR REPLACE ROW ACCESS POLICY transaction_fraud_policy AS (fraud_flag BOOLEAN) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE_ADMIN', 'ACCOUNTADMIN', 
                                 'FINANCE_ANALYST', 'SECURITY_AUDITOR')
            THEN TRUE
        ELSE fraud_flag = FALSE                             -- Hide fraudulent transactions
    END;

-- Apply row access policies
ALTER TABLE customers               ADD ROW ACCESS POLICY customer_row_policy    ON (is_active);
ALTER TABLE employees               ADD ROW ACCESS POLICY employee_row_policy    ON (termination_date);
ALTER TABLE financial_transactions  ADD ROW ACCESS POLICY transaction_fraud_policy ON (fraud_flag);


-- =============================================================================
-- VERIFY POLICIES
-- =============================================================================
SHOW MASKING POLICIES IN SCHEMA DEMO_SCHEMA;
SHOW ROW ACCESS POLICIES IN SCHEMA DEMO_SCHEMA;

-- View masking policy references
SELECT * FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'SECURITY_DEMO_DB.DEMO_SCHEMA.SSN_MASKING_POLICY'
));
