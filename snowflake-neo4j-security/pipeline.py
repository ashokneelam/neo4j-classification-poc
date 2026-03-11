"""
================================================================================
Snowflake -> Neo4j Ingestion Pipeline
Project: Data Classification & Access Control Demo

This pipeline:
1. Connects to Snowflake and extracts data + tag metadata
2. Builds a rich Neo4j graph with:
   - Database, Schema, Table, Column nodes
   - Data records (Customer, Employee, Product, etc.)
   - Classification nodes (PII, Restricted, Internal, Public)
   - Role nodes with permission relationships
   - Policy nodes (masking + row access)
3. Enforces access control at node level via classification properties

Dependencies:
    pip install snowflake-connector-python neo4j python-dotenv pandas
================================================================================
"""

import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import snowflake.connector
from neo4j import GraphDatabase
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingestion.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# Snowflake connection config (set in .env or environment)
# Strip .snowflakecomputing.com suffix if the full URL was provided in SNOWFLAKE_ACCOUNT
_sf_account = os.getenv("SNOWFLAKE_ACCOUNT", "your-account.us-east-1")
if _sf_account.endswith(".snowflakecomputing.com"):
    _sf_account = _sf_account[: -len(".snowflakecomputing.com")]

SNOWFLAKE_CONFIG = {
    "account":    _sf_account,
    "user":       os.getenv("SNOWFLAKE_USER",       "your_username"),
    "password":   os.getenv("SNOWFLAKE_PASSWORD",   "your_password"),
    "database":   os.getenv("SNOWFLAKE_DATABASE",   "SECURITY_DEMO_DB"),
    "schema":     os.getenv("SNOWFLAKE_SCHEMA",     "DEMO_SCHEMA"),
    "warehouse":  os.getenv("SNOWFLAKE_WAREHOUSE",  "COMPUTE_WH"),
    "role":       os.getenv("SNOWFLAKE_ROLE",       "DATA_ENGINEER"),
}

# Neo4j connection config
NEO4J_CONFIG = {
    "uri":      os.getenv("NEO4J_URI",      "bolt://localhost:7687"),
    "user":     os.getenv("NEO4J_USER",     "neo4j"),
    "password": os.getenv("NEO4J_PASSWORD", "password"),
}

# Tables to ingest
TARGET_TABLES = ["CUSTOMERS", "FINANCIAL_TRANSACTIONS", "EMPLOYEES", "PRODUCTS", "AUDIT_LOGS"]

# Classification hierarchy (lower = more sensitive)
CLASSIFICATION_RANK = {
    "Restricted": 1,
    "PII":        2,
    "Internal":   3,
    "Public":     4,
}

# Role -> allowed classifications mapping
ROLE_ACCESS_MAP = {
    "DATA_GOVERNANCE_ADMIN": ["Public", "Internal", "PII", "Restricted"],
    "DATA_ENGINEER":         ["Public", "Internal", "PII", "Restricted"],
    "FINANCE_ANALYST":       ["Public", "Internal", "Restricted"],
    "HR_MANAGER":            ["Public", "Internal", "PII"],
    "SECURITY_AUDITOR":      ["Public", "Internal"],
    "DATA_ANALYST":          ["Public", "Internal"],
    "PUBLIC_USER":           ["Public"],
}


# ---------------------------------------------------------------------------
# Snowflake Extractor
# ---------------------------------------------------------------------------
class SnowflakeExtractor:
    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        log.info("Connecting to Snowflake...")
        self.conn = snowflake.connector.connect(**self.config)
        log.info("[OK] Snowflake connection established")

    def disconnect(self):
        if self.conn:
            self.conn.close()
            log.info("Snowflake connection closed")

    def query(self, sql: str, params=None) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def extract_column_metadata(self) -> pd.DataFrame:
        """Extract all column metadata with tag values.
        Uses TAG_REFERENCES_ALL_COLUMNS + pivot instead of SYSTEM$GET_TAG,
        which does not accept dynamic column references in a SELECT statement.
        """
        log.info("Extracting column metadata and tags from Snowflake...")
        sql = """
        SELECT
            t.object_name                                                           AS table_name,
            t.column_name,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.ORDINAL_POSITION,
            MAX(CASE WHEN t.tag_name = 'DATA_CLASSIFICATION'  THEN t.tag_value END) AS data_classification,
            MAX(CASE WHEN t.tag_name = 'DATA_CATEGORY'        THEN t.tag_value END) AS data_category,
            MAX(CASE WHEN t.tag_name = 'ENCRYPTION_REQUIRED'  THEN t.tag_value END) AS encryption_required,
            MAX(CASE WHEN t.tag_name = 'RETENTION_POLICY'     THEN t.tag_value END) AS retention_policy,
            MAX(CASE WHEN t.tag_name = 'DATA_OWNER'           THEN t.tag_value END) AS data_owner
        FROM (
            SELECT tag_name, tag_value, object_name, column_name
            FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS', 'table'))
            UNION ALL
            SELECT tag_name, tag_value, object_name, column_name
            FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                'SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS', 'table'))
            UNION ALL
            SELECT tag_name, tag_value, object_name, column_name
            FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                'SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES', 'table'))
            UNION ALL
            SELECT tag_name, tag_value, object_name, column_name
            FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                'SECURITY_DEMO_DB.DEMO_SCHEMA.PRODUCTS', 'table'))
            UNION ALL
            SELECT tag_name, tag_value, object_name, column_name
            FROM TABLE(SECURITY_DEMO_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                'SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS', 'table'))
        ) t
        JOIN SECURITY_DEMO_DB.INFORMATION_SCHEMA.COLUMNS c
            ON  c.TABLE_NAME   = t.object_name
            AND c.COLUMN_NAME  = t.column_name
            AND c.TABLE_SCHEMA = 'DEMO_SCHEMA'
        GROUP BY t.object_name, t.column_name, c.DATA_TYPE, c.IS_NULLABLE, c.ORDINAL_POSITION
        ORDER BY t.object_name, c.ORDINAL_POSITION
        """
        df = self.query(sql)
        log.info(f"[OK] Extracted metadata for {len(df)} columns across {df['table_name'].nunique()} tables")
        return df

    def extract_table_data(self, table_name: str) -> pd.DataFrame:
        """Extract all rows from a table."""
        log.info(f"Extracting data from {table_name}...")
        df = self.query(f"SELECT * FROM {table_name} LIMIT 1000")
        log.info(f"  [OK] {len(df)} rows from {table_name}")
        return df

    def extract_masking_policies(self) -> pd.DataFrame:
        """Extract masking policy references."""
        sql = """
        SELECT 
            ref_entity_name AS table_name,
            ref_column_name AS column_name,
            policy_name,
            policy_kind,
            policy_status
        FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
            ref_entity_domain => 'TABLE',
            ref_entity_name => 'SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS'
        ))
        """
        try:
            return self.query(sql)
        except Exception as e:
            log.warning(f"Could not extract masking policies: {e}")
            return pd.DataFrame()


# ---------------------------------------------------------------------------
# Neo4j Graph Builder
# ---------------------------------------------------------------------------
class Neo4jGraphBuilder:
    def __init__(self, config: dict):
        self.driver = GraphDatabase.driver(
            config["uri"],
            auth=(config["user"], config["password"])
        )

    def close(self):
        self.driver.close()

    def run(self, cypher: str, params: dict = None):
        with self.driver.session() as session:
            return session.run(cypher, params or {}).data()

    def run_batch(self, cypher: str, batch: list):
        with self.driver.session() as session:
            session.run(cypher, {"batch": batch})

    # ------------------------------------------------------------------
    # Schema Constraints & Indexes
    # ------------------------------------------------------------------
    def create_constraints(self):
        log.info("Creating Neo4j constraints and indexes...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Database)       REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Schema)         REQUIRE s.fqn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table)          REQUIRE t.fqn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (col:Column)       REQUIRE col.fqn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Classification) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Role)           REQUIRE r.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Policy)         REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (cu:Customer)      REQUIRE cu.customer_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (em:Employee)      REQUIRE em.employee_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (pr:Product)       REQUIRE pr.product_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (tx:Transaction)   REQUIRE tx.transaction_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (al:AuditLog)      REQUIRE al.log_id IS UNIQUE",
        ]
        for c in constraints:
            self.run(c)
        log.info(f"[OK] Created {len(constraints)} constraints")

    def clear_graph(self):
        log.info("Clearing existing graph...")
        self.run("MATCH (n) DETACH DELETE n")
        log.info("[OK] Graph cleared")

    # ------------------------------------------------------------------
    # Catalog / Structural Nodes
    # ------------------------------------------------------------------
    def create_catalog_nodes(self):
        log.info("Creating catalog structure (Database -> Schema -> Tables)...")

        # Database node
        self.run("""
            MERGE (d:Database {name: $name})
            SET d.platform = 'Snowflake',
                d.created_at = $ts,
                d.environment = 'Production'
        """, {"name": "SECURITY_DEMO_DB", "ts": datetime.now(timezone.utc).isoformat()})

        # Schema node
        self.run("""
            MATCH (d:Database {name: 'SECURITY_DEMO_DB'})
            MERGE (s:Schema {fqn: 'SECURITY_DEMO_DB.DEMO_SCHEMA'})
            SET s.name = 'DEMO_SCHEMA', s.database = 'SECURITY_DEMO_DB'
            MERGE (d)-[:CONTAINS_SCHEMA]->(s)
        """)

        # Table nodes
        for table in TARGET_TABLES:
            self.run("""
                MATCH (s:Schema {fqn: 'SECURITY_DEMO_DB.DEMO_SCHEMA'})
                MERGE (t:Table {fqn: $fqn})
                SET t.name = $name,
                    t.schema = 'DEMO_SCHEMA',
                    t.database = 'SECURITY_DEMO_DB',
                    t.source_platform = 'Snowflake',
                    t.ingested_at = $ts
                MERGE (s)-[:CONTAINS_TABLE]->(t)
            """, {
                "fqn": f"SECURITY_DEMO_DB.DEMO_SCHEMA.{table}",
                "name": table,
                "ts": datetime.now(timezone.utc).isoformat()
            })
        log.info("[OK] Catalog structure created")

    # ------------------------------------------------------------------
    # Classification Nodes
    # ------------------------------------------------------------------
    def create_classification_nodes(self):
        log.info("Creating Classification nodes...")
        classifications = [
            {"name": "Restricted", "description": "Highest sensitivity — financial/legal/credential data",
             "color": "#DC2626", "rank": 1, "requires_encryption": True,
             "access_roles": ["DATA_GOVERNANCE_ADMIN", "DATA_ENGINEER", "FINANCE_ANALYST"]},
            {"name": "PII", "description": "Personally Identifiable Information — GDPR/CCPA regulated",
             "color": "#EA580C", "rank": 2, "requires_encryption": True,
             "access_roles": ["DATA_GOVERNANCE_ADMIN", "DATA_ENGINEER", "HR_MANAGER"]},
            {"name": "Internal", "description": "Internal business data — employees only",
             "color": "#D97706", "rank": 3, "requires_encryption": False,
             "access_roles": ["DATA_GOVERNANCE_ADMIN", "DATA_ENGINEER", "DATA_ANALYST",
                              "HR_MANAGER", "FINANCE_ANALYST", "SECURITY_AUDITOR"]},
            {"name": "Public", "description": "Publicly available information — no restrictions",
             "color": "#16A34A", "rank": 4, "requires_encryption": False,
             "access_roles": ["DATA_GOVERNANCE_ADMIN", "DATA_ENGINEER", "DATA_ANALYST",
                              "HR_MANAGER", "FINANCE_ANALYST", "SECURITY_AUDITOR", "PUBLIC_USER"]},
        ]
        for c in classifications:
            self.run("""
                MERGE (cl:Classification {name: $name})
                SET cl.description = $description,
                    cl.color = $color,
                    cl.sensitivity_rank = $rank,
                    cl.requires_encryption = $requires_encryption,
                    cl.access_roles = $access_roles
            """, c)
        log.info(f"[OK] Created {len(classifications)} classification nodes")

    # ------------------------------------------------------------------
    # Role & Permission Nodes
    # ------------------------------------------------------------------
    def create_role_nodes(self):
        log.info("Creating Role nodes and permission relationships...")
        roles = [
            {"name": "DATA_GOVERNANCE_ADMIN", "description": "Full governance access",
             "hierarchy_level": 1, "can_manage_policies": True},
            {"name": "DATA_ENGINEER",         "description": "Full read access to all data",
             "hierarchy_level": 2, "can_manage_policies": False},
            {"name": "FINANCE_ANALYST",       "description": "Restricted financial data access",
             "hierarchy_level": 3, "can_manage_policies": False},
            {"name": "HR_MANAGER",            "description": "Employee PII access",
             "hierarchy_level": 3, "can_manage_policies": False},
            {"name": "SECURITY_AUDITOR",      "description": "Audit log and metadata access",
             "hierarchy_level": 3, "can_manage_policies": False},
            {"name": "DATA_ANALYST",          "description": "Internal and Public data only",
             "hierarchy_level": 4, "can_manage_policies": False},
            {"name": "PUBLIC_USER",           "description": "Public data access only",
             "hierarchy_level": 5, "can_manage_policies": False},
        ]
        for r in roles:
            self.run("""
                MERGE (role:Role {name: $name})
                SET role.description = $description,
                    role.hierarchy_level = $hierarchy_level,
                    role.can_manage_policies = $can_manage_policies
            """, r)

        # Create CAN_ACCESS relationships between Roles and Classifications
        for role_name, allowed in ROLE_ACCESS_MAP.items():
            for classification in allowed:
                self.run("""
                    MATCH (r:Role {name: $role}), (c:Classification {name: $cls})
                    MERGE (r)-[:CAN_ACCESS]->(c)
                """, {"role": role_name, "cls": classification})

        # Role hierarchy relationships
        hierarchy = [
            ("DATA_GOVERNANCE_ADMIN", "DATA_ENGINEER"),
            ("DATA_ENGINEER",         "DATA_ANALYST"),
            ("DATA_ANALYST",          "HR_MANAGER"),
            ("DATA_ANALYST",          "FINANCE_ANALYST"),
            ("DATA_ANALYST",          "SECURITY_AUDITOR"),
            ("DATA_ANALYST",          "PUBLIC_USER"),
        ]
        for parent, child in hierarchy:
            self.run("""
                MATCH (p:Role {name: $parent}), (c:Role {name: $child})
                MERGE (p)-[:INHERITS_FROM]->(c)
            """, {"parent": parent, "child": child})

        log.info(f"[OK] Created {len(roles)} role nodes with permission graph")

    # ------------------------------------------------------------------
    # Column Nodes with Tag Metadata
    # ------------------------------------------------------------------
    def create_column_nodes(self, metadata_df: pd.DataFrame):
        log.info("Creating Column nodes with Snowflake tag metadata...")
        batch = []
        for _, row in metadata_df.iterrows():
            batch.append({
                "fqn":                 f"SECURITY_DEMO_DB.DEMO_SCHEMA.{row['table_name']}.{row['column_name']}",
                "name":                row['column_name'],
                "table_name":          row['table_name'],
                "table_fqn":           f"SECURITY_DEMO_DB.DEMO_SCHEMA.{row['table_name']}",
                "data_type":           row.get('data_type', 'UNKNOWN'),
                "is_nullable":         row.get('is_nullable', 'YES'),
                "ordinal_position":    int(row.get('ordinal_position', 0)),
                "data_classification": row.get('data_classification') or 'Unclassified',
                "data_category":       row.get('data_category') or '',
                "encryption_required": row.get('encryption_required') or 'No',
                "retention_policy":    row.get('retention_policy') or '',
                "data_owner":          row.get('data_owner') or '',
                "snowflake_tagged":    True,
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS col
            MATCH (t:Table {fqn: col.table_fqn})
            MERGE (c:Column {fqn: col.fqn})
            SET c.name               = col.name,
                c.table_name         = col.table_name,
                c.data_type          = col.data_type,
                c.is_nullable        = col.is_nullable,
                c.ordinal_position   = col.ordinal_position,
                c.data_classification= col.data_classification,
                c.data_category      = col.data_category,
                c.encryption_required= col.encryption_required,
                c.retention_policy   = col.retention_policy,
                c.data_owner         = col.data_owner,
                c.snowflake_tagged   = col.snowflake_tagged,
                c.ingested_at        = col.ingested_at
            MERGE (t)-[:HAS_COLUMN]->(c)
        """, batch)

        # Link columns to their Classification nodes
        self.run("""
            MATCH (c:Column)
            WHERE c.data_classification IS NOT NULL AND c.data_classification <> 'Unclassified'
            MATCH (cl:Classification {name: c.data_classification})
            MERGE (c)-[:CLASSIFIED_AS]->(cl)
        """)

        log.info(f"[OK] Created {len(batch)} column nodes with tag metadata propagated from Snowflake")

    # ------------------------------------------------------------------
    # Data Record Nodes
    # ------------------------------------------------------------------
    def ingest_customers(self, df: pd.DataFrame):
        log.info(f"Ingesting {len(df)} Customer nodes...")

        # Determine the highest sensitivity level for each row
        # based on column classifications
        col_classifications = {
            "customer_id": "Internal", "first_name": "PII", "last_name": "PII",
            "email": "PII", "phone": "PII", "date_of_birth": "PII",
            "ssn": "Restricted", "address_line1": "PII", "city": "Internal",
            "state": "Internal", "country": "Public", "customer_tier": "Internal",
        }

        batch = []
        for _, row in df.iterrows():
            batch.append({
                "customer_id":     int(row["customer_id"]),
                "first_name":      str(row["first_name"]),
                "last_name":       str(row["last_name"]),
                "email":           str(row["email"]),
                "city":            str(row.get("city", "")),
                "state":           str(row.get("state", "")),
                "country":         str(row.get("country", "USA")),
                "customer_tier":   str(row.get("customer_tier", "")),
                "is_active":       bool(row.get("is_active", True)),
                # Node-level classification = most sensitive column
                "data_classification": "Restricted",  # Because SSN is Restricted
                "pii_fields":          ["first_name", "last_name", "email", "phone",
                                        "date_of_birth", "address_line1"],
                "restricted_fields":   ["ssn"],
                "source_table":        "CUSTOMERS",
                "source_platform":     "Snowflake",
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS row
            MERGE (c:Customer {customer_id: row.customer_id})
            SET c.first_name          = row.first_name,
                c.last_name           = row.last_name,
                c.email               = row.email,
                c.city                = row.city,
                c.state               = row.state,
                c.country             = row.country,
                c.customer_tier       = row.customer_tier,
                c.is_active           = row.is_active,
                c.data_classification = row.data_classification,
                c.pii_fields          = row.pii_fields,
                c.restricted_fields   = row.restricted_fields,
                c.source_table        = row.source_table,
                c.source_platform     = row.source_platform,
                c.ingested_at         = row.ingested_at
        """, batch)

        # Link customers to Classification node
        self.run("""
            MATCH (c:Customer), (cl:Classification {name: c.data_classification})
            MERGE (c)-[:HAS_CLASSIFICATION]->(cl)
        """)
        log.info("[OK] Customer nodes created and classified")

    def ingest_employees(self, df: pd.DataFrame):
        log.info(f"Ingesting {len(df)} Employee nodes...")
        batch = []
        for _, row in df.iterrows():
            batch.append({
                "employee_id":    int(row["employee_id"]),
                "employee_number":str(row["employee_number"]),
                "first_name":     str(row["first_name"]),
                "last_name":      str(row["last_name"]),
                "email":          str(row["email"]),
                "department":     str(row.get("department", "")),
                "job_title":      str(row.get("job_title", "")),
                "location_office":str(row.get("location_office", "")),
                "remote_work":    bool(row.get("remote_work", False)),
                "is_active":      row.get("termination_date") is None or str(row.get("termination_date")) == "None",
                "data_classification": "Restricted",  # Salary+SSN = Restricted
                "pii_fields":          ["first_name", "last_name", "email", "personal_email",
                                        "phone", "date_of_birth"],
                "restricted_fields":   ["ssn", "salary", "bonus", "bank_account", "clearance_level"],
                "source_table":        "EMPLOYEES",
                "source_platform":     "Snowflake",
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS row
            MERGE (e:Employee {employee_id: row.employee_id})
            SET e.employee_number   = row.employee_number,
                e.first_name        = row.first_name,
                e.last_name         = row.last_name,
                e.email             = row.email,
                e.department        = row.department,
                e.job_title         = row.job_title,
                e.location_office   = row.location_office,
                e.remote_work       = row.remote_work,
                e.is_active         = row.is_active,
                e.data_classification = row.data_classification,
                e.pii_fields        = row.pii_fields,
                e.restricted_fields = row.restricted_fields,
                e.source_table      = row.source_table,
                e.source_platform   = row.source_platform,
                e.ingested_at       = row.ingested_at
        """, batch)

        # Manager relationships
        for _, row in df.iterrows():
            if row.get("manager_id") and str(row.get("manager_id")) != "None":
                self.run("""
                    MATCH (e:Employee {employee_id: $eid}),
                          (m:Employee {employee_id: $mid})
                    MERGE (e)-[:REPORTS_TO]->(m)
                """, {"eid": int(row["employee_id"]), "mid": int(row["manager_id"])})

        # Link to Classification
        self.run("""
            MATCH (e:Employee), (cl:Classification {name: e.data_classification})
            MERGE (e)-[:HAS_CLASSIFICATION]->(cl)
        """)
        log.info("[OK] Employee nodes created with management hierarchy")

    def ingest_products(self, df: pd.DataFrame):
        log.info(f"Ingesting {len(df)} Product nodes...")
        batch = []
        for _, row in df.iterrows():
            batch.append({
                "product_id":       int(row["product_id"]),
                "product_sku":      str(row["product_sku"]),
                "product_name":     str(row["product_name"]),
                "category":         str(row.get("category", "")),
                "subcategory":      str(row.get("subcategory", "")),
                "unit_price":       float(row.get("unit_price", 0)),
                "is_active":        bool(row.get("is_active", True)),
                "data_classification": "Restricted",  # cost_price + margin = Restricted
                "public_fields":       ["product_sku", "product_name", "product_description",
                                        "category", "subcategory"],
                "restricted_fields":   ["cost_price", "profit_margin"],
                "source_table":        "PRODUCTS",
                "source_platform":     "Snowflake",
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS row
            MERGE (p:Product {product_id: row.product_id})
            SET p.product_sku        = row.product_sku,
                p.product_name       = row.product_name,
                p.category           = row.category,
                p.subcategory        = row.subcategory,
                p.unit_price         = row.unit_price,
                p.is_active          = row.is_active,
                p.data_classification= row.data_classification,
                p.public_fields      = row.public_fields,
                p.restricted_fields  = row.restricted_fields,
                p.source_table       = row.source_table,
                p.source_platform    = row.source_platform,
                p.ingested_at        = row.ingested_at
        """, batch)

        self.run("""
            MATCH (p:Product), (cl:Classification {name: p.data_classification})
            MERGE (p)-[:HAS_CLASSIFICATION]->(cl)
        """)
        log.info("[OK] Product nodes created")

    def ingest_transactions(self, df: pd.DataFrame):
        log.info(f"Ingesting {len(df)} Transaction nodes...")
        batch = []
        for _, row in df.iterrows():
            batch.append({
                "transaction_id":   str(row["transaction_id"]),
                "customer_id":      int(row["customer_id"]),
                "transaction_type": str(row.get("transaction_type", "")),
                "merchant_name":    str(row.get("merchant_name", "")),
                "merchant_category":str(row.get("merchant_category", "")),
                "currency":         str(row.get("currency", "USD")),
                "status":           str(row.get("status", "")),
                "fraud_flag":       bool(row.get("fraud_flag", False)),
                "data_classification": "Restricted",
                "restricted_fields":   ["account_number", "routing_number",
                                        "transaction_amount", "fraud_flag", "ip_address"],
                "source_table":        "FINANCIAL_TRANSACTIONS",
                "source_platform":     "Snowflake",
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS row
            MERGE (tx:Transaction {transaction_id: row.transaction_id})
            SET tx.customer_id        = row.customer_id,
                tx.transaction_type   = row.transaction_type,
                tx.merchant_name      = row.merchant_name,
                tx.merchant_category  = row.merchant_category,
                tx.currency           = row.currency,
                tx.status             = row.status,
                tx.fraud_flag         = row.fraud_flag,
                tx.data_classification= row.data_classification,
                tx.restricted_fields  = row.restricted_fields,
                tx.source_table       = row.source_table,
                tx.source_platform    = row.source_platform,
                tx.ingested_at        = row.ingested_at
        """, batch)

        # MADE_TRANSACTION relationships
        self.run("""
            MATCH (tx:Transaction), (c:Customer {customer_id: tx.customer_id})
            MERGE (c)-[:MADE_TRANSACTION]->(tx)
        """)

        self.run("""
            MATCH (tx:Transaction), (cl:Classification {name: tx.data_classification})
            MERGE (tx)-[:HAS_CLASSIFICATION]->(cl)
        """)
        log.info("[OK] Transaction nodes created with customer relationships")

    def ingest_audit_logs(self, df: pd.DataFrame):
        log.info(f"Ingesting {len(df)} AuditLog nodes...")
        batch = []
        for _, row in df.iterrows():
            batch.append({
                "log_id":            str(row["log_id"]),
                "user_id":           str(row.get("user_id", "") or ""),
                "action_type":       str(row.get("action_type", "")),
                "resource_accessed": str(row.get("resource_accessed", "")),
                "data_classification_accessed": str(row.get("data_classification", "")),
                "success_flag":      bool(row.get("success_flag", True)),
                "failure_reason":    str(row.get("failure_reason", "") or ""),
                "data_classification": "Internal",
                "source_table":      "AUDIT_LOGS",
                "source_platform":   "Snowflake",
                "ingested_at":       datetime.now(timezone.utc).isoformat(),
            })

        self.run_batch("""
            UNWIND $batch AS row
            MERGE (al:AuditLog {log_id: row.log_id})
            SET al.user_id            = row.user_id,
                al.action_type        = row.action_type,
                al.resource_accessed  = row.resource_accessed,
                al.data_classification_accessed = row.data_classification_accessed,
                al.success_flag       = row.success_flag,
                al.failure_reason     = row.failure_reason,
                al.data_classification= row.data_classification,
                al.source_table       = row.source_table,
                al.source_platform    = row.source_platform,
                al.ingested_at        = row.ingested_at
        """, batch)

        self.run("""
            MATCH (al:AuditLog), (cl:Classification {name: al.data_classification})
            MERGE (al)-[:HAS_CLASSIFICATION]->(cl)
        """)
        log.info("[OK] AuditLog nodes created")

    # ------------------------------------------------------------------
    # Policy Nodes
    # ------------------------------------------------------------------
    def create_policy_nodes(self):
        log.info("Creating Policy nodes...")
        policies = [
            {"name": "SSN_MASKING_POLICY",    "type": "MASKING",     "applies_to": "Restricted",
             "description": "Masks SSN — full for DATA_ENGINEER, last-4 for HR, blocked otherwise"},
            {"name": "EMAIL_MASKING_POLICY",  "type": "MASKING",     "applies_to": "PII",
             "description": "Masks email — full for HR/Engineer, domain-only for Analyst"},
            {"name": "AMOUNT_MASKING_POLICY", "type": "MASKING",     "applies_to": "Restricted",
             "description": "Rounds financial amounts for Analysts, exact for Finance"},
            {"name": "ACCOUNT_MASKING_POLICY","type": "MASKING",     "applies_to": "Restricted",
             "description": "Shows only last-4 of account numbers for Finance role"},
            {"name": "IP_MASKING_POLICY",     "type": "MASKING",     "applies_to": "Restricted",
             "description": "IP address visible only to Security Auditors and Engineers"},
            {"name": "SALARY_MASKING_POLICY", "type": "MASKING",     "applies_to": "Restricted",
             "description": "Salary visible only to HR and Finance"},
            {"name": "CUSTOMER_ROW_POLICY",   "type": "ROW_ACCESS",  "applies_to": "Internal",
             "description": "Analysts see only active customers; privileged see all"},
            {"name": "EMPLOYEE_ROW_POLICY",   "type": "ROW_ACCESS",  "applies_to": "Internal",
             "description": "Active employees for Analysts; HR sees all including terminated"},
            {"name": "TRANSACTION_FRAUD_POLICY","type":"ROW_ACCESS", "applies_to": "Restricted",
             "description": "Fraud-flagged transactions visible only to Finance/Security/Engineer"},
        ]
        for p in policies:
            self.run("""
                MERGE (pol:Policy {name: $name})
                SET pol.type          = $type,
                    pol.applies_to    = $applies_to,
                    pol.description   = $description,
                    pol.source_platform = 'Snowflake'
            """, p)

        # Link policies to columns/tables
        policy_column_map = {
            "SSN_MASKING_POLICY":     ["SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS.SSN",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES.SSN"],
            "EMAIL_MASKING_POLICY":   ["SECURITY_DEMO_DB.DEMO_SCHEMA.CUSTOMERS.EMAIL",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES.EMAIL",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES.PERSONAL_EMAIL",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS.USER_EMAIL"],
            "AMOUNT_MASKING_POLICY":  ["SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS.TRANSACTION_AMOUNT"],
            "ACCOUNT_MASKING_POLICY": ["SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS.ACCOUNT_NUMBER"],
            "SALARY_MASKING_POLICY":  ["SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES.SALARY",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.EMPLOYEES.BONUS"],
            "IP_MASKING_POLICY":      ["SECURITY_DEMO_DB.DEMO_SCHEMA.FINANCIAL_TRANSACTIONS.IP_ADDRESS",
                                       "SECURITY_DEMO_DB.DEMO_SCHEMA.AUDIT_LOGS.SOURCE_IP"],
        }
        for policy_name, column_fqns in policy_column_map.items():
            for fqn in column_fqns:
                self.run("""
                    MATCH (pol:Policy {name: $policy}), (c:Column {fqn: $fqn})
                    MERGE (pol)-[:MASKS]->(c)
                """, {"policy": policy_name, "fqn": fqn})

        log.info(f"[OK] Created {len(policies)} policy nodes")

    # ------------------------------------------------------------------
    # Access Control Check (Cypher Procedure)
    # ------------------------------------------------------------------
    def create_access_control_procedure(self):
        """
        Creates a Neo4j node property convention for access checking.
        In production, this would be a Neo4j stored procedure or APOC macro.
        Here we document the Cypher pattern for access checks.
        """
        log.info("Documenting access control Cypher patterns...")
        # This is stored as a reference node in the graph
        self.run("""
            MERGE (ac:AccessControlConfig {name: 'DEFAULT'})
            SET ac.check_pattern = 
                'MATCH (r:Role {name: $role})-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n) RETURN n',
            ac.description = 
                'Check: MATCH role -> CAN_ACCESS -> Classification <- HAS_CLASSIFICATION <- DataNode',
            ac.version = '1.0',
            ac.created_at = $ts
        """, {"ts": datetime.now(timezone.utc).isoformat()})


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------
def run_pipeline(use_mock: bool = False):
    """
    Run the full ingestion pipeline.
    Set use_mock=True to run with synthetic data without Snowflake connection.
    """
    log.info("=" * 70)
    log.info("Starting Snowflake -> Neo4j Ingestion Pipeline")
    log.info("=" * 70)

    neo4j = Neo4jGraphBuilder(NEO4J_CONFIG)

    try:
        # Setup
        neo4j.clear_graph()
        neo4j.create_constraints()
        neo4j.create_catalog_nodes()
        neo4j.create_classification_nodes()
        neo4j.create_role_nodes()

        if use_mock:
            log.info("Running in MOCK mode — using embedded synthetic data")
            metadata_df, data_frames = generate_mock_data()
        else:
            # Connect to Snowflake
            sf = SnowflakeExtractor(SNOWFLAKE_CONFIG)
            sf.connect()
            try:
                metadata_df = sf.extract_column_metadata()
                data_frames = {
                    "CUSTOMERS":               sf.extract_table_data("CUSTOMERS"),
                    "EMPLOYEES":               sf.extract_table_data("EMPLOYEES"),
                    "PRODUCTS":                sf.extract_table_data("PRODUCTS"),
                    "FINANCIAL_TRANSACTIONS":  sf.extract_table_data("FINANCIAL_TRANSACTIONS"),
                    "AUDIT_LOGS":              sf.extract_table_data("AUDIT_LOGS"),
                }
            finally:
                sf.disconnect()

        # Build graph
        neo4j.create_column_nodes(metadata_df)
        neo4j.ingest_customers(data_frames["CUSTOMERS"])
        neo4j.ingest_employees(data_frames["EMPLOYEES"])
        neo4j.ingest_products(data_frames["PRODUCTS"])
        neo4j.ingest_transactions(data_frames["FINANCIAL_TRANSACTIONS"])
        neo4j.ingest_audit_logs(data_frames["AUDIT_LOGS"])
        neo4j.create_policy_nodes()
        neo4j.create_access_control_procedure()

        log.info("=" * 70)
        log.info("[DONE]  INGESTION COMPLETE")
        log.info("=" * 70)
        print_graph_summary(neo4j)

    finally:
        neo4j.close()


def print_graph_summary(neo4j: Neo4jGraphBuilder):
    """Print a summary of what was ingested."""
    result = neo4j.run("""
        MATCH (n) 
        RETURN labels(n)[0] AS label, count(n) AS count
        ORDER BY count DESC
    """)
    log.info("\n[SUMMARY] Graph Summary:")
    log.info(f"{'Node Label':<30} {'Count':>8}")
    log.info("-" * 40)
    for record in result:
        log.info(f"  {record['label']:<28} {record['count']:>8}")

    rel_result = neo4j.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel_type, count(r) AS count
        ORDER BY count DESC
    """)
    log.info("\n[RELS] Relationship Summary:")
    for record in rel_result:
        log.info(f"  {record['rel_type']:<35} {record['count']:>6}")


def generate_mock_data() -> tuple:
    """Generate mock DataFrames for testing without Snowflake."""
    # Metadata dataframe (column metadata with classification tags)
    columns_data = []
    tables_cols = {
        "CUSTOMERS": [
            ("CUSTOMER_ID", "NUMBER",   "Internal",   ""),
            ("FIRST_NAME",  "VARCHAR",  "PII",        "Personal"),
            ("LAST_NAME",   "VARCHAR",  "PII",        "Personal"),
            ("EMAIL",       "VARCHAR",  "PII",        "Personal"),
            ("PHONE",       "VARCHAR",  "PII",        "Personal"),
            ("SSN",         "VARCHAR",  "Restricted", "Personal"),
            ("CITY",        "VARCHAR",  "Internal",   ""),
            ("COUNTRY",     "VARCHAR",  "Public",     ""),
            ("CUSTOMER_TIER","VARCHAR", "Internal",   ""),
            ("IS_ACTIVE",   "BOOLEAN",  "Internal",   ""),
        ],
        "EMPLOYEES": [
            ("EMPLOYEE_ID",   "NUMBER",  "Internal",   ""),
            ("FIRST_NAME",    "VARCHAR", "PII",        "Personal"),
            ("LAST_NAME",     "VARCHAR", "PII",        "Personal"),
            ("EMAIL",         "VARCHAR", "PII",        "Personal"),
            ("SSN",           "VARCHAR", "Restricted", "Personal"),
            ("SALARY",        "NUMBER",  "Restricted", "Financial"),
            ("BONUS",         "NUMBER",  "Restricted", "Financial"),
            ("DEPARTMENT",    "VARCHAR", "Internal",   "Operational"),
            ("LOCATION_OFFICE","VARCHAR","Public",     ""),
            ("CLEARANCE_LEVEL","VARCHAR","Restricted", ""),
        ],
        "FINANCIAL_TRANSACTIONS": [
            ("TRANSACTION_ID",   "VARCHAR","Internal",  ""),
            ("ACCOUNT_NUMBER",   "VARCHAR","Restricted","Financial"),
            ("TRANSACTION_AMOUNT","NUMBER","Restricted","Financial"),
            ("FRAUD_FLAG",       "BOOLEAN","Restricted","Financial"),
            ("MERCHANT_CATEGORY","VARCHAR","Public",    ""),
            ("CURRENCY",         "VARCHAR","Public",    ""),
            ("IP_ADDRESS",       "VARCHAR","Restricted","Personal"),
        ],
        "PRODUCTS": [
            ("PRODUCT_ID",    "NUMBER",  "Internal",  ""),
            ("PRODUCT_SKU",   "VARCHAR", "Public",    ""),
            ("PRODUCT_NAME",  "VARCHAR", "Public",    ""),
            ("COST_PRICE",    "NUMBER",  "Restricted","Financial"),
            ("PROFIT_MARGIN", "NUMBER",  "Restricted","Financial"),
            ("UNIT_PRICE",    "NUMBER",  "Internal",  ""),
            ("CATEGORY",      "VARCHAR", "Public",    ""),
        ],
        "AUDIT_LOGS": [
            ("LOG_ID",          "VARCHAR","Internal",  ""),
            ("USER_EMAIL",      "VARCHAR","PII",       "Personal"),
            ("SOURCE_IP",       "VARCHAR","Restricted",""),
            ("ACTION_TYPE",     "VARCHAR","Internal",  ""),
            ("SUCCESS_FLAG",    "BOOLEAN","Internal",  ""),
        ],
    }

    for table, cols in tables_cols.items():
        for i, (col_name, dtype, classification, category) in enumerate(cols):
            columns_data.append({
                "table_name":          table,
                "column_name":         col_name,
                "data_type":           dtype,
                "is_nullable":         "YES",
                "ordinal_position":    i + 1,
                "data_classification": classification,
                "data_category":       category,
                "encryption_required": "Yes" if classification in ("Restricted", "PII") else "No",
                "retention_policy":    "7_years" if classification == "Restricted" else "",
                "data_owner":          "Compliance Team" if classification == "Restricted" else "",
            })
    metadata_df = pd.DataFrame(columns_data)

    # Customers
    customers = pd.DataFrame([
        {"customer_id": 1001, "first_name": "Alice", "last_name": "Johnson",
         "email": "alice.johnson@email.com", "city": "Austin", "state": "TX",
         "country": "USA", "customer_tier": "Gold", "is_active": True},
        {"customer_id": 1002, "first_name": "Bob", "last_name": "Martinez",
         "email": "bob.martinez@email.com", "city": "Seattle", "state": "WA",
         "country": "USA", "customer_tier": "Silver", "is_active": True},
        {"customer_id": 1003, "first_name": "Carol", "last_name": "Williams",
         "email": "carol.w@email.com", "city": "Chicago", "state": "IL",
         "country": "USA", "customer_tier": "Platinum", "is_active": True},
        {"customer_id": 1004, "first_name": "David", "last_name": "Brown",
         "email": "david.brown@email.com", "city": "Miami", "state": "FL",
         "country": "USA", "customer_tier": "Bronze", "is_active": True},
        {"customer_id": 1005, "first_name": "Emma", "last_name": "Davis",
         "email": "emma.davis@email.com", "city": "Denver", "state": "CO",
         "country": "USA", "customer_tier": "Gold", "is_active": False},
    ])

    # Employees
    employees = pd.DataFrame([
        {"employee_id": 5001, "employee_number": "EMP-001", "first_name": "Sarah", "last_name": "Chen",
         "email": "sarah.chen@company.com", "department": "Engineering", "job_title": "Senior Engineer",
         "manager_id": 5010, "location_office": "San Francisco HQ", "remote_work": False,
         "termination_date": None},
        {"employee_id": 5002, "employee_number": "EMP-002", "first_name": "Marcus", "last_name": "Thompson",
         "email": "marcus.t@company.com", "department": "Marketing", "job_title": "Marketing Manager",
         "manager_id": 5010, "location_office": "New York Office", "remote_work": False,
         "termination_date": None},
        {"employee_id": 5010, "employee_number": "EMP-010", "first_name": "Robert", "last_name": "Kim",
         "email": "robert.kim@company.com", "department": "Executive", "job_title": "CTO",
         "manager_id": None, "location_office": "San Francisco HQ", "remote_work": False,
         "termination_date": None},
        {"employee_id": 5008, "employee_number": "EMP-008", "first_name": "Tyler", "last_name": "Brooks",
         "email": "tyler.brooks@company.com", "department": "Marketing", "job_title": "Marketing Associate",
         "manager_id": 5002, "location_office": "Austin Office", "remote_work": False,
         "termination_date": "2024-06-30"},
    ])

    # Products
    products = pd.DataFrame([
        {"product_id": 101, "product_sku": "SKU-LAPTOP-PRO", "product_name": "ProBook Laptop 15\"",
         "category": "Electronics", "subcategory": "Computers", "unit_price": 1499.99, "is_active": True},
        {"product_id": 102, "product_sku": "SKU-PHONE-X1", "product_name": "SmartPhone X1",
         "category": "Electronics", "subcategory": "Mobile", "unit_price": 999.99, "is_active": True},
        {"product_id": 103, "product_sku": "SKU-HEADPHONE-BT", "product_name": "ProSound BT Headphones",
         "category": "Electronics", "subcategory": "Audio", "unit_price": 349.99, "is_active": True},
    ])

    # Transactions
    transactions = pd.DataFrame([
        {"transaction_id": "txn-uuid-0001", "customer_id": 1001, "transaction_type": "Purchase",
         "merchant_name": "Amazon", "merchant_category": "E-Commerce", "currency": "USD",
         "status": "Completed", "fraud_flag": False},
        {"transaction_id": "txn-uuid-0002", "customer_id": 1002, "transaction_type": "Purchase",
         "merchant_name": "Whole Foods", "merchant_category": "Grocery", "currency": "USD",
         "status": "Completed", "fraud_flag": False},
        {"transaction_id": "txn-uuid-0010", "customer_id": 1003, "transaction_type": "Purchase",
         "merchant_name": "Rolex Boutique", "merchant_category": "Luxury", "currency": "USD",
         "status": "Completed", "fraud_flag": True},
    ])

    # Audit logs
    audit_logs = pd.DataFrame([
        {"log_id": "log-001", "user_id": "user:sarah.chen", "action_type": "SELECT",
         "resource_accessed": "customers.email", "data_classification": "PII", "success_flag": True,
         "failure_reason": None},
        {"log_id": "log-002", "user_id": "user:marcus.t", "action_type": "SELECT",
         "resource_accessed": "customers.*", "data_classification": "PII", "success_flag": False,
         "failure_reason": "Insufficient permissions"},
        {"log_id": "log-008", "user_id": "user:unknown", "action_type": "SELECT",
         "resource_accessed": "customers.ssn", "data_classification": "Restricted", "success_flag": False,
         "failure_reason": "Authentication failed"},
    ])

    data_frames = {
        "CUSTOMERS":              customers,
        "EMPLOYEES":              employees,
        "PRODUCTS":               products,
        "FINANCIAL_TRANSACTIONS": transactions,
        "AUDIT_LOGS":             audit_logs,
    }
    return metadata_df, data_frames


if __name__ == "__main__":
    import sys
    mock_mode = "--mock" in sys.argv
    run_pipeline(use_mock=mock_mode)
