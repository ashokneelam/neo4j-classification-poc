// =============================================================================
// NEO4J ACCESS CONTROL DEMO QUERIES
// Project: Snowflake → Neo4j Data Classification & Security Demo
//
// Run these in Neo4j Browser or Cypher Shell to demonstrate:
//   1. Node-level access control by classification
//   2. Role-level security
//   3. Column-level security (field filtering)
// =============================================================================


// =============================================================================
// SECTION 1: EXPLORE THE GRAPH STRUCTURE
// =============================================================================

// 1a. View the full classification hierarchy
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)
RETURN r.name AS role, collect(cl.name) AS accessible_classifications
ORDER BY r.hierarchy_level;

// 1b. View all nodes and their classifications
MATCH (n)-[:HAS_CLASSIFICATION]->(cl:Classification)
WHERE n:Customer OR n:Employee OR n:Product OR n:Transaction OR n:AuditLog
RETURN labels(n)[0] AS node_type, cl.name AS classification, count(n) AS count
ORDER BY cl.sensitivity_rank, node_type;

// 1c. View the schema (columns with tags)
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN t.name AS table_name, 
       c.name AS column_name,
       c.data_classification AS classification,
       c.data_category AS category,
       c.encryption_required AS encrypted,
       c.data_owner AS owner
ORDER BY t.name, c.ordinal_position;

// 1d. View policy → column relationships
MATCH (p:Policy)-[:MASKS]->(c:Column)
RETURN p.name AS policy, p.type AS policy_type, 
       c.table_name AS table_name, c.name AS column_name,
       c.data_classification AS classification;


// =============================================================================
// SECTION 2: NODE-LEVEL ACCESS CONTROL SIMULATION
// Based on: Role → CAN_ACCESS → Classification ← HAS_CLASSIFICATION ← DataNode
// =============================================================================

// ── DEMO: What can DATA_ANALYST see? ──────────────────────────────────────────
// DATA_ANALYST can access: Internal + Public only
MATCH (r:Role {name: 'DATA_ANALYST'})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(n)
RETURN labels(n)[0] AS node_type, count(n) AS accessible_nodes,
       collect(distinct cl.name) AS classifications
ORDER BY node_type;
// RESULT: Only nodes with Internal or Public classification visible


// ── DEMO: What can DATA_ENGINEER see? ─────────────────────────────────────────
// DATA_ENGINEER: All classifications
MATCH (r:Role {name: 'DATA_ENGINEER'})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(n)
RETURN labels(n)[0] AS node_type, count(n) AS accessible_nodes
ORDER BY node_type;
// RESULT: ALL nodes visible (Restricted + PII + Internal + Public)


// ── DEMO: Access-controlled customer list for a role ──────────────────────────
// Parameterize with different roles to see the effect
// Replace $role_name with: 'PUBLIC_USER', 'DATA_ANALYST', 'HR_MANAGER', 'DATA_ENGINEER'

WITH 'DATA_ANALYST' AS current_role   // ← change this to test different roles
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(c:Customer)
RETURN c.customer_id, c.first_name, c.last_name, c.city, 
       c.customer_tier, c.data_classification AS node_classification,
       current_role AS accessing_as
ORDER BY c.customer_id;
// PUBLIC_USER      → 0 results (Customer nodes are Restricted)
// DATA_ANALYST     → 0 results (no access to Restricted)
// HR_MANAGER       → 0 results (no access to Restricted — PII only)
// FINANCE_ANALYST  → all customers (has Restricted access)
// DATA_ENGINEER    → all customers (has Restricted access)


// ── DEMO: Finance-only transaction view ───────────────────────────────────────
WITH 'FINANCE_ANALYST' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(tx:Transaction)
RETURN tx.transaction_id, tx.merchant_name, tx.merchant_category,
       tx.fraud_flag, tx.status, tx.data_classification,
       current_role AS accessing_as
ORDER BY tx.transaction_id;


// ── DEMO: ACCESS DENIED example ───────────────────────────────────────────────
// Try to access a Restricted node as PUBLIC_USER
WITH 'PUBLIC_USER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(n)
RETURN labels(n)[0] AS node_type, n, cl.name AS classification
LIMIT 10;
// Should return 0 nodes with Restricted/PII/Internal classification
// Public users see nothing from our current dataset (all nodes are Internal+)


// =============================================================================
// SECTION 3: COLUMN-LEVEL SECURITY (FIELD MASKING SIMULATION)
// =============================================================================

// Neo4j doesn't natively mask properties, but we simulate it by:
// 1. Querying columns the role CAN access
// 2. Returning only those properties from the data node

// ── DEMO: Column-level security for HR_MANAGER viewing employees ───────────────
// Step 1: Determine which columns HR_MANAGER can see
WITH 'HR_MANAGER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:CLASSIFIED_AS]-(col:Column {table_name: 'EMPLOYEES'})
RETURN current_role AS role, 
       collect(col.name + ' (' + col.data_classification + ')') AS accessible_columns
ORDER BY role;

// Step 2: Fetch employee data with role-based field projection
// HR_MANAGER can see PII + Internal + Public but NOT Restricted (salary/SSN/bank)
WITH 'HR_MANAGER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(e:Employee)
RETURN 
    e.employee_id      AS id,
    e.first_name       AS first_name,          // ✓ PII (accessible to HR)
    e.last_name        AS last_name,           // ✓ PII
    e.email            AS email,               // ✓ PII
    e.department       AS department,          // ✓ Internal
    e.job_title        AS job_title,           // ✓ Internal
    e.location_office  AS location,            // ✓ Public
    '[MASKED - Restricted]' AS salary,         // ✗ Restricted - masked
    '[MASKED - Restricted]' AS ssn,            // ✗ Restricted - masked
    current_role       AS accessing_as;


// ── DEMO: DATA_ENGINEER sees everything ───────────────────────────────────────
WITH 'DATA_ENGINEER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(e:Employee)
RETURN 
    e.employee_id      AS id,
    e.first_name       AS first_name,
    e.last_name        AS last_name,
    e.department       AS department,
    e.job_title        AS job_title,
    'VISIBLE (Restricted)' AS salary_note,    // ✓ Restricted - visible
    'VISIBLE (Restricted)' AS ssn_note,       // ✓ Restricted - visible
    current_role       AS accessing_as;


// ── DEMO: FINANCE_ANALYST sees financial columns ──────────────────────────────
WITH 'FINANCE_ANALYST' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)
<-[:CLASSIFIED_AS]-(col:Column)
WHERE col.table_name = 'FINANCIAL_TRANSACTIONS'
RETURN col.table_name, col.name AS column_name, 
       col.data_classification AS classification,
       CASE col.data_classification 
            WHEN 'Public' THEN '✓ VISIBLE - No masking'
            WHEN 'Internal' THEN '✓ VISIBLE - No masking'
            WHEN 'Restricted' THEN '✓ VISIBLE - Masking policy applies'
            ELSE '✗ BLOCKED'
       END AS access_decision
ORDER BY col.ordinal_position;


// =============================================================================
// SECTION 4: ROLE-LEVEL SECURITY DEMO
// =============================================================================

// ── DEMO: Role hierarchy traversal ───────────────────────────────────────────
MATCH path = (r:Role {name: 'DATA_GOVERNANCE_ADMIN'})-[:INHERITS_FROM*]->(child:Role)
RETURN [node IN nodes(path) | node.name] AS role_chain;

// ── DEMO: Find all roles that can access a specific classification ─────────────
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification {name: 'Restricted'})
RETURN r.name AS role, r.description AS description, r.hierarchy_level AS level
ORDER BY r.hierarchy_level;

// ── DEMO: Unauthorized access detection (simulated) ───────────────────────────
// Simulate: DATA_ANALYST tries to access Restricted data
WITH 'DATA_ANALYST' AS current_role, 'Restricted' AS requested_classification
MATCH (r:Role {name: current_role})
OPTIONAL MATCH (r)-[:CAN_ACCESS]->(cl:Classification {name: requested_classification})
RETURN 
    current_role AS role,
    requested_classification AS classification_requested,
    CASE WHEN cl IS NULL 
         THEN '🚫 ACCESS DENIED: Role does not have permission'
         ELSE '✅ ACCESS GRANTED'
    END AS access_decision;


// =============================================================================
// SECTION 5: SENSITIVE DATA LINEAGE
// =============================================================================

// ── Find all Restricted nodes and who can access them ────────────────────────
MATCH (cl:Classification {name: 'Restricted'})<-[:HAS_CLASSIFICATION]-(n)
WITH cl, labels(n)[0] AS node_type, count(n) AS node_count
MATCH (r:Role)-[:CAN_ACCESS]->(cl)
RETURN cl.name AS classification, node_type, node_count,
       collect(r.name) AS roles_with_access
ORDER BY node_type;

// ── PII exposure map ──────────────────────────────────────────────────────────
MATCH (col:Column {data_classification: 'PII'})
MATCH (t:Table)
WHERE t.fqn = 'SECURITY_DEMO_DB.DEMO_SCHEMA.' + col.table_name
RETURN t.name AS table_name, 
       collect(col.name) AS pii_columns,
       count(col) AS pii_column_count
ORDER BY pii_column_count DESC;

// ── Encryption audit ──────────────────────────────────────────────────────────
MATCH (col:Column)
WHERE col.data_classification IN ['Restricted', 'PII']
  AND col.encryption_required = 'No'
RETURN col.table_name, col.name, col.data_classification,
       '⚠️  COMPLIANCE RISK: Sensitive column not marked for encryption' AS warning;

// ── Data owner accountability ─────────────────────────────────────────────────
MATCH (col:Column)
WHERE col.data_owner IS NOT NULL AND col.data_owner <> ''
RETURN col.data_owner AS owner_team, 
       collect(col.table_name + '.' + col.name) AS owned_columns,
       count(col) AS column_count
ORDER BY column_count DESC;


// =============================================================================
// SECTION 6: FULL ACCESS AUDIT TRAIL
// =============================================================================

// ── Simulate a complete access request audit ──────────────────────────────────
// "Can FINANCE_ANALYST see transaction amounts for customer 1001?"
WITH 'FINANCE_ANALYST' AS role_name
MATCH (r:Role {name: role_name})-[:CAN_ACCESS]->(cl:Classification)
<-[:HAS_CLASSIFICATION]-(c:Customer {customer_id: 1001})
-[:MADE_TRANSACTION]->(tx:Transaction)
RETURN 
    role_name AS role,
    c.customer_id AS customer_id,
    tx.transaction_id AS transaction_id,
    tx.merchant_name AS merchant,
    tx.merchant_category AS category,
    '✓ ACCESSIBLE (amount masked by Snowflake policy)' AS amount_access,
    cl.name AS classification_granted
ORDER BY tx.transaction_id;


// =============================================================================
// SECTION 7: GRAPH VISUALIZATION QUERIES
// Use in Neo4j Browser with "Graph" view mode
// =============================================================================

// ── Full policy graph ─────────────────────────────────────────────────────────
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n)
WHERE n:Customer OR n:Employee
RETURN r, cl, n LIMIT 30;

// ── Classification → Column relationship graph ────────────────────────────────
MATCH (col:Column)-[:CLASSIFIED_AS]->(cl:Classification)
RETURN col, cl LIMIT 50;

// ── Employee management hierarchy ─────────────────────────────────────────────
MATCH path = (e:Employee)-[:REPORTS_TO*0..3]->(m:Employee)
WHERE NOT (m)-[:REPORTS_TO]->()
RETURN path;

// ── Customer → Transaction graph ─────────────────────────────────────────────
MATCH path = (c:Customer)-[:MADE_TRANSACTION]->(tx:Transaction)
RETURN path LIMIT 20;
