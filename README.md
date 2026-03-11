# Snowflake → Neo4j Data Classification & Access Control Demo

**Repository:** [https://github.com/ashokneelam/neo4j-classification-poc](https://github.com/ashokneelam/neo4j-classification-poc)

A complete end-to-end project demonstrating:
- **Column-level tagging** in Snowflake (PII, Restricted, Internal, Public)
- **ETL ingestion** that propagates tags as Neo4j node properties
- **Node-level access control** in Neo4j via classification graph
- **Role-level security** with 7 roles and hierarchy
- **Column-level masking** via Snowflake policies mirrored in Neo4j

---

## Project Structure

```
neo4j-classification-poc/
├── snowflake-neo4j-security/
│   ├── 01_setup_tags.sql               # Create classification tags in Snowflake
│   ├── 02_create_tables.sql            # 5 tables with sample data
│   ├── 03_apply_tags.sql               # Apply tags to all 81 columns
│   ├── 04_security_policies.sql        # Roles, masking + row access policies
│   ├── 05_tag_extraction_queries.sql   # ETL extraction queries
│   ├── pipeline.py                     # Snowflake → Neo4j Python ingestion pipeline
│   ├── 01_access_control_queries.cypher  # Neo4j demo Cypher queries
│   ├── demo_snowflake.sql              # Live demo queries for Snowflake
│   ├── demo_neo4j.cypher               # Live demo queries for Neo4j Browser
│   ├── demo-dashboard.html             # Interactive demo UI (open in browser)
│   ├── setup_google_sheet.gs           # Google Apps Script to build demo spreadsheet
│   └── .env.example                    # Environment variable template
├── .vscode/
│   ├── settings.json                   # Python + Cypher editor config
│   └── launch.json                     # VS Code run configurations
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Quick Start

### 1. Set Up Snowflake

Run the SQL scripts in order in your Snowflake worksheet:

```sql
-- Step 1: Create tags
@snowflake-neo4j-security/01_setup_tags.sql

-- Step 2: Create tables with dummy data
@snowflake-neo4j-security/02_create_tables.sql

-- Step 3: Apply classification tags to columns
@snowflake-neo4j-security/03_apply_tags.sql

-- Step 4: Create roles, masking policies, row access policies
@snowflake-neo4j-security/04_security_policies.sql
```

### 2. Configure Environment

```bash
cp snowflake-neo4j-security/.env.example .env
# Edit .env with your Snowflake and Neo4j credentials
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Ingestion Pipeline

```bash
# With live Snowflake connection:
python snowflake-neo4j-security/pipeline.py

# Without Snowflake (mock data mode):
python snowflake-neo4j-security/pipeline.py --mock
```

### 5. Explore in Neo4j Browser

Open Neo4j Browser and run queries from `snowflake-neo4j-security/01_access_control_queries.cypher`.

---

## Tables & Classification Tags

| Table | Restricted | PII | Internal | Public |
|-------|-----------|-----|----------|--------|
| CUSTOMERS | ssn | first_name, last_name, email, phone, dob | customer_id, tier | country |
| EMPLOYEES | ssn, salary, bonus, bank_account, clearance_level | first_name, email, phone, dob | department, job_title | location_office |
| FINANCIAL_TRANSACTIONS | account_number, amount, fraud_flag, ip_address | — | transaction_id, type | merchant_category, currency |
| PRODUCTS | cost_price, profit_margin | — | product_id, stock | product_sku, name, category |
| AUDIT_LOGS | source_ip, session_id | user_email | log_id, action_type | — |

---

## Neo4j Graph Model

```
(:Database)
    └─[:CONTAINS_SCHEMA]→(:Schema)
          └─[:CONTAINS_TABLE]→(:Table)
                └─[:HAS_COLUMN]→(:Column)
                      └─[:CLASSIFIED_AS]→(:Classification)

(:Role)─[:CAN_ACCESS]→(:Classification)
(:Role)─[:INHERITS_FROM]→(:Role)

(:DataNode)─[:HAS_CLASSIFICATION]→(:Classification)
(:Policy)─[:MASKS]→(:Column)
```

### Node Labels
- `Database`, `Schema`, `Table`, `Column` — catalog/structural nodes
- `Customer`, `Employee`, `Product`, `Transaction`, `AuditLog` — data nodes
- `Classification` — PII, Restricted, Internal, Public
- `Role` — 7 roles with hierarchy
- `Policy` — 9 policies (6 masking + 3 row access)

---

## Role Hierarchy & Permissions

```
DATA_GOVERNANCE_ADMIN  (Level 1)  → All classifications
    └── DATA_ENGINEER             → All classifications
            └── DATA_ANALYST      → Internal + Public only
                    ├── HR_MANAGER        → PII + Internal + Public
                    ├── FINANCE_ANALYST   → Restricted + Internal + Public
                    ├── SECURITY_AUDITOR  → Internal + Public
                    └── PUBLIC_USER       → Public only
```

---

## Access Control Demo Queries

```cypher
-- What can DATA_ANALYST see?
MATCH (r:Role {name: 'DATA_ANALYST'})-[:CAN_ACCESS]->(cl:Classification)
      <-[:HAS_CLASSIFICATION]-(n)
RETURN labels(n)[0] AS type, count(n) AS count

-- Column-level: which columns can HR_MANAGER see in EMPLOYEES?
MATCH (r:Role {name: 'HR_MANAGER'})-[:CAN_ACCESS]->(cl:Classification)
      <-[:CLASSIFIED_AS]-(col:Column {table_name: 'EMPLOYEES'})
RETURN col.name, col.data_classification, col.data_owner

-- Access denied simulation
WITH 'PUBLIC_USER' AS role, 'Restricted' AS cls
MATCH (r:Role {name: role})
OPTIONAL MATCH (r)-[:CAN_ACCESS]->(cl:Classification {name: cls})
RETURN CASE WHEN cl IS NULL THEN 'DENIED' ELSE 'GRANTED' END AS decision
```

---

## Masking Policies (Snowflake → Neo4j)

| Policy | Applies To | Behavior |
|--------|-----------|---------|
| SSN_MASKING | ssn columns | Full for Engineer; last-4 for HR; masked for others |
| EMAIL_MASKING | email columns | Full for HR/Engineer; domain-only for Analyst |
| AMOUNT_MASKING | transaction_amount | Exact for Finance; rounded for Analyst; -1 blocked |
| ACCOUNT_MASKING | account_number | Full for Engineer; last-4 for Finance |
| IP_MASKING | ip_address, source_ip | Visible to Security Auditor/Engineer only |
| SALARY_MASKING | salary, bonus | Visible to HR/Finance only |

---

## Row Access Policies

| Policy | Table | Rule |
|--------|-------|------|
| CUSTOMER_ROW_POLICY | CUSTOMERS | Analysts see active customers only |
| EMPLOYEE_ROW_POLICY | EMPLOYEES | Analysts see non-terminated only |
| TRANSACTION_FRAUD_POLICY | FINANCIAL_TRANSACTIONS | Fraud rows hidden from non-Finance/Security roles |
