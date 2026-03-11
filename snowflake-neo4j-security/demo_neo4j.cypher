// =============================================================================
// LIVE DEMO: Neo4j Side
// Snowflake -> Neo4j Data Classification & Security POC
//
// Run these in Neo4j Browser (https://browser.neo4j.io or Aura console).
// Switch to "Graph" view for the visual queries, "Table" view for tabular ones.
// Each section number matches demo_snowflake.sql for side-by-side comparison.
// =============================================================================


// =============================================================================
// SECTION 1: THE GRAPH SCHEMA — Snowflake tables become nodes & relationships
// =============================================================================

// 1a. Full catalog hierarchy: Database -> Schema -> Table -> Column
//     (Switch to GRAPH view — you'll see the full structural graph)
MATCH path = (db:Database)-[:CONTAINS_SCHEMA]->(s:Schema)-[:CONTAINS_TABLE]->(t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN path
LIMIT 80;

// 1b. Table view: every table and its column count (mirrors Snowflake INFORMATION_SCHEMA)
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN
    t.name          AS snowflake_table,
    t.fqn           AS fully_qualified_name,
    count(c)        AS column_count
ORDER BY snowflake_table;

// 1c. All columns for a specific table — compare directly with Snowflake schema
MATCH (t:Table {name: 'CUSTOMERS'})-[:HAS_COLUMN]->(c:Column)
RETURN
    c.name                  AS column_name,
    c.data_type             AS data_type,
    c.ordinal_position      AS position,
    c.data_classification   AS classification,
    c.data_category         AS category,
    c.encryption_required   AS encryption_required,
    c.data_owner            AS owner
ORDER BY position;


// =============================================================================
// SECTION 2: TAGS PROPAGATED — Snowflake column tags as node properties in Neo4j
// =============================================================================

// 2a. All Column nodes with their Snowflake tag metadata
//     (This is what the pipeline wrote from TAG_REFERENCES_ALL_COLUMNS)
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN
    t.name                  AS table_name,
    c.name                  AS column_name,
    c.data_type             AS data_type,
    c.data_classification   AS data_classification,
    c.data_category         AS data_category,
    c.encryption_required   AS encryption_required,
    c.retention_policy      AS retention_policy,
    c.data_owner            AS data_owner
ORDER BY table_name, c.ordinal_position;

// 2b. Column -> Classification relationships (graph view recommended)
//     Each CLASSIFIED_AS edge represents a tag from Snowflake
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification)
RETURN t, c, cl
LIMIT 60;

// 2c. Tag propagation proof: count columns per classification tier
//     Compare these numbers against demo_snowflake.sql Section 3a
MATCH (c:Column)-[:CLASSIFIED_AS]->(cl:Classification)
RETURN
    cl.name             AS classification,
    cl.sensitivity_rank AS sensitivity_rank,
    count(c)            AS column_count
ORDER BY sensitivity_rank DESC;


// =============================================================================
// SECTION 3: TAG COVERAGE — Sensitive column audit across the estate
// =============================================================================

// 3a. All Restricted columns — who owns them and do they require encryption?
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification {name: 'Restricted'})
RETURN
    t.name                  AS table_name,
    c.name                  AS column_name,
    c.data_owner            AS owner_team,
    c.encryption_required   AS encryption_required,
    c.retention_policy      AS retention_policy
ORDER BY table_name, column_name;

// 3b. All PII columns across all tables
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification {name: 'PII'})
RETURN
    t.name              AS table_name,
    collect(c.name)     AS pii_columns,
    count(c)            AS pii_column_count
ORDER BY pii_column_count DESC;

// 3c. Compliance risk: sensitive columns NOT flagged for encryption
MATCH (c:Column)-[:CLASSIFIED_AS]->(cl:Classification)
WHERE cl.name IN ['Restricted', 'PII']
    AND (c.encryption_required IS NULL OR c.encryption_required = 'No')
RETURN
    c.table_name            AS table_name,
    c.name                  AS column_name,
    cl.name                 AS classification,
    'COMPLIANCE RISK: encryption not enforced' AS warning;

// 3d. Data ownership map — who is accountable for what?
MATCH (c:Column)
WHERE c.data_owner IS NOT NULL
RETURN
    c.data_owner                                AS owner_team,
    count(c)                                    AS column_count,
    collect(c.table_name + '.' + c.name)        AS owned_columns
ORDER BY column_count DESC;


// =============================================================================
// SECTION 4: MASKING POLICIES — Policies as graph nodes
// =============================================================================

// 4a. All policy nodes and what classification they protect
MATCH (p:Policy)
RETURN
    p.name          AS policy_name,
    p.type          AS policy_type,
    p.database      AS database,
    p.schema        AS schema
ORDER BY policy_type, policy_name;

// 4b. Policy -> Column masking map (graph view shows the full picture)
MATCH (p:Policy)-[:MASKS]->(c:Column)
RETURN p, c
LIMIT 40;

// 4c. Table view: which columns are masked by which policy?
MATCH (p:Policy)-[:MASKS]->(c:Column)
RETURN
    p.name              AS policy,
    p.type              AS policy_type,
    c.table_name        AS table_name,
    c.name              AS column_name,
    c.data_classification AS classification
ORDER BY table_name, column_name;


// =============================================================================
// SECTION 5: ROLE-BASED ACCESS CONTROL — The security graph
// =============================================================================

// 5a. Role hierarchy (graph view — shows INHERITS_FROM chain visually)
MATCH (r:Role)
OPTIONAL MATCH (r)-[:INHERITS_FROM]->(parent:Role)
RETURN r, parent
LIMIT 20;

// 5b. Role -> Classification access permissions
//     "Which roles can see which classification tiers?"
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)
RETURN
    r.name                      AS role,
    r.hierarchy_level           AS level,
    r.description               AS description,
    collect(cl.name)            AS can_access_classifications
ORDER BY level;

// 5c. Who can access Restricted data? (the most sensitive tier)
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification {name: 'Restricted'})
RETURN
    r.name          AS role,
    r.description   AS description,
    r.hierarchy_level AS level
ORDER BY level;

// 5d. Full access control graph: Role -> Classification -> Column (graph view)
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)<-[:CLASSIFIED_AS]-(c:Column)
WHERE cl.name = 'Restricted'
RETURN r, cl, c
LIMIT 40;


// =============================================================================
// SECTION 6: ACCESS SIMULATION — "What can each role actually see?"
// =============================================================================

// 6a. What can DATA_ANALYST access? (node-level)
WITH 'DATA_ANALYST' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n)
RETURN
    labels(n)[0]        AS node_type,
    count(n)            AS accessible_nodes,
    collect(DISTINCT cl.name) AS via_classifications
ORDER BY node_type;

// 6b. What can DATA_ENGINEER access? (compare with above)
WITH 'DATA_ENGINEER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n)
RETURN
    labels(n)[0]        AS node_type,
    count(n)            AS accessible_nodes
ORDER BY node_type;

// 6c. Column-level visibility for HR_MANAGER on EMPLOYEES table
//     Shows which columns HR can see vs. which are masked
WITH 'HR_MANAGER' AS current_role
MATCH (r:Role {name: current_role})-[:CAN_ACCESS]->(cl:Classification)<-[:CLASSIFIED_AS]-(c:Column {table_name: 'EMPLOYEES'})
WITH current_role, collect(c.name) AS visible_columns
MATCH (all_col:Column {table_name: 'EMPLOYEES'})
RETURN
    all_col.name                AS column_name,
    all_col.data_classification AS classification,
    CASE WHEN all_col.name IN visible_columns
        THEN 'VISIBLE'
        ELSE 'MASKED / NO ACCESS'
    END                         AS access_decision,
    current_role                AS role
ORDER BY all_col.ordinal_position;

// 6d. ACCESS DENIED simulation — DATA_ANALYST tries to access Restricted data
WITH 'DATA_ANALYST' AS role_name, 'Restricted' AS requested_class
MATCH (r:Role {name: role_name})
OPTIONAL MATCH (r)-[:CAN_ACCESS]->(cl:Classification {name: requested_class})
RETURN
    role_name                   AS role,
    requested_class             AS classification_requested,
    CASE
        WHEN cl IS NULL THEN 'ACCESS DENIED: Role does not have permission'
        ELSE 'ACCESS GRANTED'
    END                         AS access_decision;


// =============================================================================
// SECTION 7: FULL AUDIT QUESTION — The "so what" of the whole demo
// =============================================================================

// 7a. "Can FINANCE_ANALYST see transaction amounts for customer transactions?"
//     Graph traversal from role -> classification -> customer -> transactions
WITH 'FINANCE_ANALYST' AS role_name
MATCH
    (r:Role {name: role_name})-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-
    (tx:Transaction)
RETURN
    role_name                                           AS role,
    count(tx)                                           AS accessible_transactions,
    cl.name                                             AS via_classification,
    'Amount masked in Snowflake by amount_masking_policy' AS note;

// 7b. Sensitive data exposure map — all Restricted/PII nodes + every role that can see them
MATCH (cl:Classification)<-[:HAS_CLASSIFICATION]-(n)
WHERE cl.name IN ['Restricted', 'PII']
WITH cl, labels(n)[0] AS node_type, count(n) AS node_count
MATCH (r:Role)-[:CAN_ACCESS]->(cl)
RETURN
    cl.name                 AS classification,
    node_type,
    node_count,
    collect(r.name)         AS roles_with_access
ORDER BY cl.name, node_type;

// 7c. End-to-end lineage: Role -> Classification -> Column -> Policy
//     "Which policies protect the columns that FINANCE_ANALYST can access?"
MATCH
    (r:Role {name: 'FINANCE_ANALYST'})-[:CAN_ACCESS]->(cl:Classification)
    <-[:CLASSIFIED_AS]-(c:Column)
    <-[:MASKS]-(p:Policy)
RETURN
    r.name          AS role,
    cl.name         AS classification,
    c.table_name    AS table_name,
    c.name          AS column_name,
    p.name          AS masking_policy
ORDER BY table_name, column_name;

// 7d. Full graph: complete security picture in one visual
//     (Run in Neo4j Browser GRAPH view for the demo money shot)
MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)
MATCH (cl)<-[:CLASSIFIED_AS]-(c:Column)
OPTIONAL MATCH (p:Policy)-[:MASKS]->(c)
RETURN r, cl, c, p
LIMIT 50;
