-- =============================================================================
-- STEP 2: CREATE TABLES WITH DUMMY DATA
-- Project: Snowflake → Neo4j Data Classification & Security Demo
-- =============================================================================

USE DATABASE SECURITY_DEMO_DB;
USE SCHEMA DEMO_SCHEMA;
USE ROLE SYSADMIN;

-- =============================================================================
-- TABLE 1: CUSTOMERS (Mix of PII, Internal, Public)
-- =============================================================================
CREATE OR REPLACE TABLE customers (
    customer_id     NUMBER(10)      NOT NULL,          -- Internal
    first_name      VARCHAR(100)    NOT NULL,          -- PII
    last_name       VARCHAR(100)    NOT NULL,          -- PII
    email           VARCHAR(255)    NOT NULL,          -- PII
    phone           VARCHAR(20),                       -- PII
    date_of_birth   DATE,                              -- PII (Sensitive)
    ssn             VARCHAR(11),                       -- Restricted (Most Sensitive)
    address_line1   VARCHAR(255),                      -- PII
    address_line2   VARCHAR(255),                      -- PII
    city            VARCHAR(100),                      -- Internal
    state           VARCHAR(50),                       -- Internal
    zip_code        VARCHAR(10),                       -- Internal
    country         VARCHAR(100)    DEFAULT 'USA',     -- Public
    customer_tier   VARCHAR(20),                       -- Internal
    created_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(), -- Internal
    is_active       BOOLEAN         DEFAULT TRUE       -- Internal
);

-- Insert dummy customer data
INSERT INTO customers VALUES
(1001, 'Alice',   'Johnson',  'alice.johnson@email.com',   '555-0101', '1985-03-15', '123-45-6789', '123 Oak Street',    NULL,        'Austin',    'TX', '78701', 'USA', 'Gold',    '2020-01-15 09:00:00', TRUE),
(1002, 'Bob',     'Martinez', 'bob.martinez@email.com',    '555-0102', '1990-07-22', '234-56-7890', '456 Elm Avenue',    'Apt 2B',    'Seattle',   'WA', '98101', 'USA', 'Silver',  '2020-02-20 10:30:00', TRUE),
(1003, 'Carol',   'Williams', 'carol.w@email.com',         '555-0103', '1978-11-08', '345-67-8901', '789 Pine Road',     NULL,        'Chicago',   'IL', '60601', 'USA', 'Platinum','2019-11-05 14:00:00', TRUE),
(1004, 'David',   'Brown',    'david.brown@email.com',     '555-0104', '1995-01-30', '456-78-9012', '321 Maple Drive',   'Suite 100', 'Miami',     'FL', '33101', 'USA', 'Bronze',  '2021-03-10 08:45:00', TRUE),
(1005, 'Emma',    'Davis',    'emma.davis@email.com',      '555-0105', '1988-09-14', '567-89-0123', '654 Cedar Lane',    NULL,        'Denver',    'CO', '80201', 'USA', 'Gold',    '2020-06-18 16:20:00', FALSE),
(1006, 'Frank',   'Miller',   'frank.miller@email.com',    '555-0106', '1972-05-25', '678-90-1234', '987 Birch Blvd',    NULL,        'Portland',  'OR', '97201', 'USA', 'Silver',  '2019-08-22 11:10:00', TRUE),
(1007, 'Grace',   'Wilson',   'grace.wilson@email.com',    '555-0107', '1993-12-03', '789-01-2345', '147 Walnut Way',    'Unit 5',    'Nashville', 'TN', '37201', 'USA', 'Gold',    '2021-01-07 13:30:00', TRUE),
(1008, 'Henry',   'Moore',    'henry.moore@email.com',     '555-0108', '1982-04-17', '890-12-3456', '258 Spruce Court',  NULL,        'Phoenix',   'AZ', '85001', 'USA', 'Platinum','2018-12-15 09:55:00', TRUE),
(1009, 'Iris',    'Taylor',   'iris.taylor@email.com',     '555-0109', '1997-08-29', '901-23-4567', '369 Ash Street',    'Apt 3C',    'Boston',    'MA', '02101', 'USA', 'Bronze',  '2022-04-12 15:40:00', TRUE),
(1010, 'Jack',    'Anderson', 'jack.anderson@email.com',   '555-0110', '1969-02-11', '012-34-5678', '741 Cherry Path',   NULL,        'San Diego', 'CA', '92101', 'USA', 'Silver',  '2019-05-30 10:00:00', TRUE);


-- =============================================================================
-- TABLE 2: FINANCIAL_TRANSACTIONS (Restricted + PII)
-- =============================================================================
CREATE OR REPLACE TABLE financial_transactions (
    transaction_id      VARCHAR(36)     NOT NULL,      -- Internal
    customer_id         NUMBER(10)      NOT NULL,      -- Internal (FK)
    account_number      VARCHAR(20)     NOT NULL,      -- Restricted
    routing_number      VARCHAR(9),                    -- Restricted
    card_last_four      VARCHAR(4),                    -- Restricted
    transaction_amount  NUMBER(12, 2)   NOT NULL,      -- Restricted
    transaction_type    VARCHAR(50),                   -- Internal
    merchant_name       VARCHAR(255),                  -- Internal
    merchant_category   VARCHAR(100),                  -- Public
    transaction_date    TIMESTAMP_NTZ,                 -- Internal
    currency            VARCHAR(3)      DEFAULT 'USD', -- Public
    status              VARCHAR(20),                   -- Internal
    fraud_flag          BOOLEAN         DEFAULT FALSE, -- Restricted
    ip_address          VARCHAR(45),                   -- Restricted (PII)
    device_fingerprint  VARCHAR(255)                   -- Restricted
);

INSERT INTO financial_transactions VALUES
('txn-uuid-0001', 1001, 'ACC-100100001', '021000021', '4532', 1250.00,  'Purchase',   'Amazon',           'E-Commerce',    '2024-01-15 10:23:00', 'USD', 'Completed', FALSE, '192.168.1.101', 'fp-abc123'),
('txn-uuid-0002', 1002, 'ACC-100200002', '021000021', '7896', 89.99,    'Purchase',   'Whole Foods',      'Grocery',       '2024-01-15 12:05:00', 'USD', 'Completed', FALSE, '10.0.0.45',     'fp-def456'),
('txn-uuid-0003', 1003, 'ACC-100300003', '121000358', '2341', 5000.00,  'Transfer',   'Wire Transfer',    'Banking',       '2024-01-16 09:00:00', 'USD', 'Completed', FALSE, '172.16.0.22',   'fp-ghi789'),
('txn-uuid-0004', 1001, 'ACC-100100001', '021000021', '4532', 45.50,    'Purchase',   'Starbucks',        'Food & Drink',  '2024-01-16 08:15:00', 'USD', 'Completed', FALSE, '192.168.1.101', 'fp-abc123'),
('txn-uuid-0005', 1004, 'ACC-100400004', '091000019', '9087', 2200.00,  'Purchase',   'Apple Store',      'Electronics',   '2024-01-17 15:30:00', 'USD', 'Completed', FALSE, '192.0.2.55',    'fp-jkl012'),
('txn-uuid-0006', 1005, 'ACC-100500005', '044000037', '1234', 750.00,   'Purchase',   'United Airlines',  'Travel',        '2024-01-17 18:45:00', 'USD', 'Declined',  FALSE, '203.0.113.10',  'fp-mno345'),
('txn-uuid-0007', 1006, 'ACC-100600006', '121000358', '5678', 15000.00, 'Transfer',   'Mortgage Payment', 'Banking',       '2024-01-18 07:00:00', 'USD', 'Completed', FALSE, '198.51.100.25', 'fp-pqr678'),
('txn-uuid-0008', 1007, 'ACC-100700007', '021000021', '3456', 320.00,   'Purchase',   'Best Buy',         'Electronics',   '2024-01-18 14:20:00', 'USD', 'Completed', FALSE, '192.168.2.200', 'fp-stu901'),
('txn-uuid-0009', 1008, 'ACC-100800008', '091000019', '7890', 99.00,    'Subscription','Netflix',         'Entertainment', '2024-01-19 00:00:00', 'USD', 'Completed', FALSE, '10.10.10.10',   'fp-vwx234'),
('txn-uuid-0010', 1003, 'ACC-100300003', '121000358', '2341', 12500.00, 'Purchase',   'Rolex Boutique',   'Luxury',        '2024-01-19 13:10:00', 'USD', 'Completed', TRUE,  '172.16.0.22',   'fp-ghi789');


-- =============================================================================
-- TABLE 3: EMPLOYEES (PII + Restricted + Internal)
-- =============================================================================
CREATE OR REPLACE TABLE employees (
    employee_id         NUMBER(10)      NOT NULL,      -- Internal
    employee_number     VARCHAR(20)     NOT NULL,      -- Internal
    first_name          VARCHAR(100)    NOT NULL,      -- PII
    last_name           VARCHAR(100)    NOT NULL,      -- PII
    email               VARCHAR(255)    NOT NULL,      -- PII
    personal_email      VARCHAR(255),                  -- PII
    phone               VARCHAR(20),                   -- PII
    ssn                 VARCHAR(11),                   -- Restricted
    date_of_birth       DATE,                          -- PII
    hire_date           DATE,                          -- Internal
    termination_date    DATE,                          -- Internal
    department          VARCHAR(100),                  -- Internal
    job_title           VARCHAR(150),                  -- Internal
    manager_id          NUMBER(10),                    -- Internal
    salary              NUMBER(12, 2),                 -- Restricted
    bonus               NUMBER(12, 2),                 -- Restricted
    bank_account        VARCHAR(20),                   -- Restricted
    performance_rating  VARCHAR(20),                   -- Internal
    location_office     VARCHAR(100),                  -- Public
    remote_work         BOOLEAN         DEFAULT FALSE, -- Internal
    clearance_level     VARCHAR(50)                    -- Restricted
);

INSERT INTO employees VALUES
(5001, 'EMP-001', 'Sarah',   'Chen',      'sarah.chen@company.com',    'sarah.chen@gmail.com',    '555-1001', '111-22-3333', '1988-06-12', '2018-03-01', NULL,         'Engineering',     'Senior Engineer',        5010, 145000.00, 20000.00, 'BANK-001-ACCT', 'Excellent',     'San Francisco HQ', FALSE, 'Level 3'),
(5002, 'EMP-002', 'Marcus',  'Thompson',  'marcus.t@company.com',      'marcus.t@yahoo.com',      '555-1002', '222-33-4444', '1992-09-25', '2020-07-15', NULL,         'Marketing',       'Marketing Manager',      5011, 110000.00, 15000.00, 'BANK-002-ACCT', 'Good',          'New York Office',  FALSE, 'Level 1'),
(5003, 'EMP-003', 'Priya',   'Patel',     'priya.patel@company.com',   'priya.patel@outlook.com', '555-1003', '333-44-5555', '1985-02-18', '2016-11-20', NULL,         'Finance',         'Financial Analyst',      5012, 125000.00, 18000.00, 'BANK-003-ACCT', 'Excellent',     'Chicago Office',   TRUE,  'Level 2'),
(5004, 'EMP-004', 'James',   'O''Brien',  'james.obrien@company.com',  'j.obrien@hotmail.com',    '555-1004', '444-55-6666', '1979-11-30', '2012-05-10', NULL,         'Sales',           'VP of Sales',            5010, 200000.00, 50000.00, 'BANK-004-ACCT', 'Excellent',     'Austin Office',    FALSE, 'Level 2'),
(5005, 'EMP-005', 'Luna',    'Rodriguez', 'luna.rodriguez@company.com', 'luna.r@gmail.com',       '555-1005', '555-66-7777', '1994-04-07', '2021-09-01', NULL,         'HR',              'HR Business Partner',    5013, 95000.00,  10000.00, 'BANK-005-ACCT', 'Good',          'San Francisco HQ', TRUE,  'Level 1'),
(5006, 'EMP-006', 'Kai',     'Nakamura',  'kai.nakamura@company.com',  'kai.n@gmail.com',         '555-1006', '666-77-8888', '1990-07-14', '2019-02-14', NULL,         'Engineering',     'DevOps Engineer',        5001, 130000.00, 12000.00, 'BANK-006-ACCT', 'Good',          'Remote',           TRUE,  'Level 3'),
(5007, 'EMP-007', 'Amara',   'Osei',      'amara.osei@company.com',    'amara.osei@outlook.com',  '555-1007', '777-88-9999', '1987-12-22', '2017-06-01', NULL,         'Legal',           'General Counsel',        5010, 250000.00, 75000.00, 'BANK-007-ACCT', 'Excellent',     'New York Office',  FALSE, 'Level 4'),
(5008, 'EMP-008', 'Tyler',   'Brooks',    'tyler.brooks@company.com',  'tyler.b@yahoo.com',       '555-1008', '888-99-0000', '1996-03-08', '2022-01-10', '2024-06-30', 'Marketing',       'Marketing Associate',    5002, 75000.00,  5000.00,  'BANK-008-ACCT', 'Satisfactory',  'Austin Office',    FALSE, 'Level 1'),
(5009, 'EMP-009', 'Fatima',  'Al-Hassan', 'fatima.alhassan@company.com','fatima.ah@gmail.com',    '555-1009', '999-00-1111', '1991-08-16', '2020-11-15', NULL,         'Data Science',    'Data Scientist',         5001, 155000.00, 22000.00, 'BANK-009-ACCT', 'Excellent',     'San Francisco HQ', TRUE,  'Level 3'),
(5010, 'EMP-010', 'Robert',  'Kim',       'robert.kim@company.com',    'robert.kim@gmail.com',    '555-1010', '000-11-2222', '1975-05-20', '2010-01-15', NULL,         'Executive',       'CTO',                    NULL, 450000.00, 200000.00,'BANK-010-ACCT', 'Exceptional',   'San Francisco HQ', FALSE, 'Level 5');


-- =============================================================================
-- TABLE 4: PRODUCTS (Mostly Public/Internal)
-- =============================================================================
CREATE OR REPLACE TABLE products (
    product_id          NUMBER(10)      NOT NULL,      -- Internal
    product_sku         VARCHAR(50)     NOT NULL,      -- Public
    product_name        VARCHAR(255)    NOT NULL,      -- Public
    product_description TEXT,                          -- Public
    category            VARCHAR(100),                  -- Public
    subcategory         VARCHAR(100),                  -- Public
    unit_price          NUMBER(10, 2),                 -- Internal
    cost_price          NUMBER(10, 2),                 -- Restricted
    profit_margin       NUMBER(5, 2),                  -- Restricted
    stock_quantity      NUMBER(10),                    -- Internal
    reorder_threshold   NUMBER(10),                    -- Internal
    supplier_id         VARCHAR(50),                   -- Internal
    is_active           BOOLEAN         DEFAULT TRUE,  -- Internal
    launch_date         DATE,                          -- Public
    last_modified       TIMESTAMP_NTZ                  -- Internal
);

INSERT INTO products VALUES
(101, 'SKU-LAPTOP-PRO',   'ProBook Laptop 15"',       'High-performance laptop for professionals',  'Electronics',    'Computers',       1499.99, 850.00,  43.33, 500,  50,  'SUP-001', TRUE, '2023-01-15', '2024-01-01 00:00:00'),
(102, 'SKU-PHONE-X1',     'SmartPhone X1',            '5G smartphone with AI camera',               'Electronics',    'Mobile',          999.99,  550.00,  45.00, 1200, 100, 'SUP-001', TRUE, '2023-06-01', '2024-01-01 00:00:00'),
(103, 'SKU-HEADPHONE-BT', 'ProSound BT Headphones',   'Noise-canceling Bluetooth headphones',       'Electronics',    'Audio',           349.99,  120.00,  65.71, 800,  80,  'SUP-002', TRUE, '2022-11-01', '2024-01-01 00:00:00'),
(104, 'SKU-DESK-CHAIR',   'ErgoComfort Chair Pro',    'Ergonomic office chair',                     'Furniture',      'Office',          599.99,  280.00,  53.33, 200,  20,  'SUP-003', TRUE, '2023-03-20', '2024-01-01 00:00:00'),
(105, 'SKU-MONITOR-4K',   '4K UltraView Monitor 27"', 'Professional 4K display monitor',           'Electronics',    'Displays',        799.99,  400.00,  50.00, 300,  30,  'SUP-001', TRUE, '2023-08-10', '2024-01-01 00:00:00'),
(106, 'SKU-KEYBOARD-MEC', 'MechType Pro Keyboard',    'Mechanical keyboard for typists',            'Electronics',    'Peripherals',     179.99,  65.00,   63.89, 600,  60,  'SUP-002', TRUE, '2022-09-15', '2024-01-01 00:00:00'),
(107, 'SKU-CAMERA-PRO',   'VisionPro DSLR Camera',    'Professional DSLR with 45MP sensor',        'Electronics',    'Photography',     2499.99, 1400.00, 44.00, 150,  15,  'SUP-004', TRUE, '2023-05-01', '2024-01-01 00:00:00'),
(108, 'SKU-TABLET-AIR',   'TabletAir 11"',            'Lightweight tablet for creativity',          'Electronics',    'Tablets',         749.99,  380.00,  49.33, 400,  40,  'SUP-001', TRUE, '2023-10-01', '2024-01-01 00:00:00'),
(109, 'SKU-COFFEE-MAKER', 'BrewMaster Pro Coffee',    'Smart WiFi coffee maker with app',           'Appliances',     'Kitchen',         299.99,  130.00,  56.67, 250,  25,  'SUP-005', TRUE, '2023-04-15', '2024-01-01 00:00:00'),
(110, 'SKU-LAMP-SMART',   'LuminaDesk Smart Lamp',    'AI-powered smart desk lamp',                 'Appliances',     'Lighting',        129.99,  45.00,   65.38, 700,  70,  'SUP-005', FALSE,'2022-07-01', '2024-01-01 00:00:00');


-- =============================================================================
-- TABLE 5: AUDIT_LOGS (Internal + Restricted)
-- =============================================================================
CREATE OR REPLACE TABLE audit_logs (
    log_id              VARCHAR(36)     NOT NULL,      -- Internal
    event_timestamp     TIMESTAMP_NTZ   NOT NULL,      -- Internal
    user_id             VARCHAR(100),                  -- Internal
    user_email          VARCHAR(255),                  -- PII
    source_ip           VARCHAR(45),                   -- Restricted
    action_type         VARCHAR(100),                  -- Internal
    resource_accessed   VARCHAR(500),                  -- Internal
    data_classification VARCHAR(50),                   -- Internal
    success_flag        BOOLEAN,                       -- Internal
    failure_reason      VARCHAR(500),                  -- Internal
    session_id          VARCHAR(100),                  -- Restricted
    user_agent          VARCHAR(500),                  -- Restricted
    records_accessed    NUMBER(10),                    -- Internal
    query_hash          VARCHAR(64)                    -- Internal
);

INSERT INTO audit_logs VALUES
('log-001', '2024-01-15 10:23:01', 'user:sarah.chen',    'sarah.chen@company.com',    '10.0.1.1',  'SELECT',  'customers.email',            'PII',        TRUE,  NULL,                       'sess-aaa111', 'Mozilla/5.0 Chrome/120',    1,  'qh-abc'),
('log-002', '2024-01-15 11:00:00', 'user:marcus.t',      'marcus.t@company.com',      '10.0.1.2',  'SELECT',  'customers.*',                'PII',        FALSE, 'Insufficient permissions', 'sess-bbb222', 'Mozilla/5.0 Chrome/120',    0,  'qh-def'),
('log-003', '2024-01-15 12:30:00', 'user:priya.patel',   'priya.patel@company.com',   '10.0.1.3',  'SELECT',  'financial_transactions.ssn', 'Restricted', TRUE,  NULL,                       'sess-ccc333', 'Mozilla/5.0 Firefox/121',   10, 'qh-ghi'),
('log-004', '2024-01-15 14:15:00', 'user:james.obrien',  'james.obrien@company.com',  '10.0.1.4',  'UPDATE',  'employees.salary',           'Restricted', FALSE, 'Unauthorized role',        'sess-ddd444', 'Mozilla/5.0 Safari/17',     0,  'qh-jkl'),
('log-005', '2024-01-16 09:00:00', 'user:fatima.ah',     'fatima.alhassan@company.com','10.0.1.9',  'SELECT',  'employees.*',                'PII',        TRUE,  NULL,                       'sess-eee555', 'Mozilla/5.0 Chrome/120',    5,  'qh-mno'),
('log-006', '2024-01-16 10:30:00', 'svc:etl_pipeline',   NULL,                        '10.0.2.50', 'SELECT',  'financial_transactions.*',   'Restricted', TRUE,  NULL,                       'sess-fff666', 'ETL-Service/1.0',           100,'qh-pqr'),
('log-007', '2024-01-16 11:45:00', 'user:robert.kim',    'robert.kim@company.com',    '10.0.1.10', 'SELECT',  'audit_logs.*',               'Internal',   TRUE,  NULL,                       'sess-ggg777', 'Mozilla/5.0 Chrome/120',    50, 'qh-stu'),
('log-008', '2024-01-17 08:20:00', 'user:unknown',       'external@suspicious.com',   '203.0.113.99','SELECT','customers.ssn',              'Restricted', FALSE, 'Authentication failed',    'sess-hhh888', 'curl/7.88.1',               0,  'qh-vwx'),
('log-009', '2024-01-17 13:00:00', 'user:luna.rodriguez','luna.rodriguez@company.com', '10.0.1.5',  'SELECT',  'employees.performance_rating','Internal',  TRUE,  NULL,                       'sess-iii999', 'Mozilla/5.0 Firefox/121',   10, 'qh-yza'),
('log-010', '2024-01-18 16:45:00', 'svc:backup_service', NULL,                        '10.0.3.100','COPY',    'ALL_TABLES',                 'Restricted', TRUE,  NULL,                       'sess-jjj000', 'BackupService/2.1',         999,'qh-bcd');

SHOW TABLES IN SCHEMA DEMO_SCHEMA;
