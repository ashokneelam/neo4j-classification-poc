"""
================================================================================
tag_sync.py  —  Snowflake Column Tag → Neo4j Incremental Sync
================================================================================

Keeps Neo4j Column node tag properties in sync with Snowflake without
clearing or re-ingesting data nodes (Customer, Employee, Product, etc.).

Safe to run at any frequency — fully idempotent.  Uses MERGE so only
properties that have actually changed are written.

Usage
-----
    # Sync tags now (Snowflake connection required):
    python snowflake-neo4j-security/tag_sync.py

    # Dry-run — shows what would change, writes nothing:
    python snowflake-neo4j-security/tag_sync.py --dry-run

    # Run every 60 minutes until interrupted:
    python snowflake-neo4j-security/tag_sync.py --watch 60

Scheduling
----------
  Cron (every hour):
    0 * * * * cd /path/to/repo && python snowflake-neo4j-security/tag_sync.py >> logs/tag_sync.log 2>&1

  Windows Task Scheduler:
    Action: python C:\\path\\to\\repo\\snowflake-neo4j-security\\tag_sync.py

  Snowflake Task (writes change log to Snowflake, triggers via stored procedure):
    See snowflake-neo4j-security/sync_task.sql

What this syncs
---------------
  Tag properties on each Column node:
    - data_classification  (PII / Restricted / Internal / Public)
    - data_category
    - encryption_required
    - retention_policy
    - data_owner

  After updating Column properties, it also refreshes CLASSIFIED_AS edges
  so that any reclassification is reflected in access-control queries
  immediately.

What this does NOT touch
------------------------
  - Data nodes  (Customer, Employee, Product, Transaction, AuditLog)
  - Role nodes and CAN_ACCESS relationships
  - Policy nodes and MASKS relationships
  - The graph schema (Database, Schema, Table nodes)
================================================================================
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Config — reuses the same .env as pipeline.py
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("tag_sync.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("tag_sync")

_sf_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
if _sf_account.endswith(".snowflakecomputing.com"):
    _sf_account = _sf_account[: -len(".snowflakecomputing.com")]

SNOWFLAKE_CONFIG = {
    "account":   _sf_account,
    "user":      os.getenv("SNOWFLAKE_USER",      ""),
    "password":  os.getenv("SNOWFLAKE_PASSWORD",  ""),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "SECURITY_DEMO_DB"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",     "DEMO_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE",  "COMPUTE_WH"),
    "role":      os.getenv("SNOWFLAKE_ROLE",       "DATA_ENGINEER"),
}

NEO4J_CONFIG = {
    "uri":      os.getenv("NEO4J_URI",      "bolt://localhost:7687"),
    "user":     os.getenv("NEO4J_USER",     "neo4j"),
    "password": os.getenv("NEO4J_PASSWORD", "password"),
}

TARGET_TABLES = [
    "CUSTOMERS",
    "EMPLOYEES",
    "FINANCIAL_TRANSACTIONS",
    "PRODUCTS",
    "AUDIT_LOGS",
]

DB   = os.getenv("SNOWFLAKE_DATABASE", "SECURITY_DEMO_DB")
SCH  = os.getenv("SNOWFLAKE_SCHEMA",   "DEMO_SCHEMA")

# ---------------------------------------------------------------------------
# Snowflake: pull current tag state
# ---------------------------------------------------------------------------

def fetch_snowflake_tags(conn) -> pd.DataFrame:
    """
    Returns one row per column with all classification tags pivoted.
    This is the same query the main pipeline uses, so output format matches.
    """
    unions = "\n    UNION ALL\n    ".join(
        f"SELECT tag_name, tag_value, object_name, column_name\n"
        f"    FROM TABLE({DB}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS("
        f"'{DB}.{SCH}.{t}', 'table'))"
        for t in TARGET_TABLES
    )
    sql = f"""
    SELECT
        t.object_name                                                            AS table_name,
        t.column_name,
        c.DATA_TYPE,
        c.IS_NULLABLE,
        c.ORDINAL_POSITION,
        MAX(CASE WHEN t.tag_name = 'DATA_CLASSIFICATION' THEN t.tag_value END) AS data_classification,
        MAX(CASE WHEN t.tag_name = 'DATA_CATEGORY'       THEN t.tag_value END) AS data_category,
        MAX(CASE WHEN t.tag_name = 'ENCRYPTION_REQUIRED' THEN t.tag_value END) AS encryption_required,
        MAX(CASE WHEN t.tag_name = 'RETENTION_POLICY'    THEN t.tag_value END) AS retention_policy,
        MAX(CASE WHEN t.tag_name = 'DATA_OWNER'          THEN t.tag_value END) AS data_owner
    FROM (
        {unions}
    ) t
    JOIN {DB}.INFORMATION_SCHEMA.COLUMNS c
        ON  c.TABLE_NAME   = t.object_name
        AND c.COLUMN_NAME  = t.column_name
        AND c.TABLE_SCHEMA = '{SCH}'
    GROUP BY t.object_name, t.column_name, c.DATA_TYPE, c.IS_NULLABLE, c.ORDINAL_POSITION
    ORDER BY t.object_name, c.ORDINAL_POSITION
    """
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Neo4j: pull current tag state
# ---------------------------------------------------------------------------

def fetch_neo4j_tags(driver) -> dict:
    """
    Returns { fqn -> {classification, category, encryption, retention, owner} }
    for every Column node currently in Neo4j.
    """
    with driver.session() as session:
        records = session.run("""
            MATCH (c:Column)
            RETURN c.fqn               AS fqn,
                   c.data_classification AS classification,
                   c.data_category       AS category,
                   c.encryption_required AS encryption,
                   c.retention_policy    AS retention,
                   c.data_owner          AS owner
        """).data()
    return {r["fqn"]: r for r in records}


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

TAG_FIELDS = {
    "data_classification": "classification",
    "data_category":       "category",
    "encryption_required": "encryption",
    "retention_policy":    "retention",
    "data_owner":          "owner",
}

def _val(v) -> str:
    return (v or "").strip()

def compute_diff(sf_df: pd.DataFrame, neo4j_map: dict) -> tuple[list, list, list]:
    """
    Returns (new_columns, changed_columns, unchanged_columns).
    Each changed entry is a dict with fqn, field, old_value, new_value.
    """
    new_cols, changed, unchanged = [], [], []

    for _, row in sf_df.iterrows():
        fqn = f"{DB}.{SCH}.{row['table_name']}.{row['column_name']}"
        neo4j_row = neo4j_map.get(fqn)

        if neo4j_row is None:
            new_cols.append(fqn)
            continue

        diffs = []
        for sf_field, n4j_field in TAG_FIELDS.items():
            sf_val  = _val(row.get(sf_field))
            n4j_val = _val(neo4j_row.get(n4j_field))
            # Treat 'Unclassified' stored in Neo4j as equivalent to empty in Snowflake
            if n4j_val == "Unclassified" and sf_val == "":
                continue
            if sf_val != n4j_val:
                diffs.append({
                    "fqn":       fqn,
                    "field":     sf_field,
                    "old_value": n4j_val or "(none)",
                    "new_value": sf_val  or "(none)",
                })
        if diffs:
            changed.extend(diffs)
        else:
            unchanged.append(fqn)

    return new_cols, changed, unchanged


# ---------------------------------------------------------------------------
# Apply updates to Neo4j
# ---------------------------------------------------------------------------

def apply_sync(driver, sf_df: pd.DataFrame, sync_ts: str) -> int:
    """
    MERGEs updated tag properties onto Column nodes.
    Rebuilds CLASSIFIED_AS edges for any reclassified columns.
    Returns number of columns written.
    """
    batch = []
    for _, row in sf_df.iterrows():
        batch.append({
            "fqn":                 f"{DB}.{SCH}.{row['table_name']}.{row['column_name']}",
            "name":                row["column_name"],
            "table_name":          row["table_name"],
            "table_fqn":           f"{DB}.{SCH}.{row['table_name']}",
            "data_type":           _val(row.get("data_type")) or "UNKNOWN",
            "is_nullable":         _val(row.get("is_nullable")) or "YES",
            "ordinal_position":    int(row.get("ordinal_position") or 0),
            "data_classification": _val(row.get("data_classification")) or "Unclassified",
            "data_category":       _val(row.get("data_category")),
            "encryption_required": _val(row.get("encryption_required")) or "No",
            "retention_policy":    _val(row.get("retention_policy")),
            "data_owner":          _val(row.get("data_owner")),
            "last_tag_sync":       sync_ts,
        })

    with driver.session() as session:
        # Update Column nodes
        session.run("""
            UNWIND $batch AS col
            MATCH (t:Table {fqn: col.table_fqn})
            MERGE (c:Column {fqn: col.fqn})
            SET c.name                = col.name,
                c.table_name          = col.table_name,
                c.data_type           = col.data_type,
                c.is_nullable         = col.is_nullable,
                c.ordinal_position    = col.ordinal_position,
                c.data_classification = col.data_classification,
                c.data_category       = col.data_category,
                c.encryption_required = col.encryption_required,
                c.retention_policy    = col.retention_policy,
                c.data_owner          = col.data_owner,
                c.snowflake_tagged    = true,
                c.last_tag_sync       = col.last_tag_sync,
                c.sync_version        = coalesce(c.sync_version, 0) + 1
            MERGE (t)-[:HAS_COLUMN]->(c)
        """, batch=batch)

        # Rebuild CLASSIFIED_AS edges (drop stale ones first)
        session.run("MATCH (:Column)-[r:CLASSIFIED_AS]->(:Classification) DELETE r")
        session.run("""
            MATCH (c:Column)
            WHERE c.data_classification IS NOT NULL
              AND c.data_classification <> 'Unclassified'
            MATCH (cl:Classification {name: c.data_classification})
            MERGE (c)-[:CLASSIFIED_AS]->(cl)
        """)

        # Stamp last sync on the global config node
        session.run("""
            MERGE (cfg:AccessControlConfig {name: 'global'})
            SET cfg.last_tag_sync  = $ts,
                cfg.synced_columns = $count
        """, ts=sync_ts, count=len(batch))

    return len(batch)


# ---------------------------------------------------------------------------
# Main sync run
# ---------------------------------------------------------------------------

def run_sync(dry_run: bool = False) -> dict:
    """
    Executes one full tag sync cycle.
    Returns a summary dict with counts.
    """
    sync_ts = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info(f"TAG SYNC  {'(DRY RUN) ' if dry_run else ''}— {sync_ts}")
    log.info("=" * 60)

    # 1. Fetch from Snowflake
    log.info("Connecting to Snowflake...")
    sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        sf_df = fetch_snowflake_tags(sf_conn)
    finally:
        sf_conn.close()
    log.info(f"  Snowflake: {len(sf_df)} columns across {sf_df['table_name'].nunique()} tables")

    # 2. Fetch from Neo4j
    log.info("Connecting to Neo4j...")
    driver = GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
    )
    try:
        neo4j_map = fetch_neo4j_tags(driver)
        log.info(f"  Neo4j   : {len(neo4j_map)} Column nodes found")

        # 3. Diff
        new_cols, changed, unchanged = compute_diff(sf_df, neo4j_map)

        log.info(f"  New columns   : {len(new_cols)}")
        log.info(f"  Changed tags  : {len(changed)}")
        log.info(f"  Unchanged     : {len(unchanged)}")

        if new_cols:
            log.info("  New columns to create:")
            for fqn in new_cols:
                log.info(f"    + {fqn}")

        if changed:
            log.info("  Tag changes detected:")
            for diff in changed:
                log.info(f"    ~ {diff['fqn']}  [{diff['field']}]  "
                         f"{diff['old_value']!r} -> {diff['new_value']!r}")

        # 4. Apply (unless dry run)
        if dry_run:
            log.info("[DRY RUN] No changes written to Neo4j.")
            written = 0
        elif not new_cols and not changed:
            log.info("No changes detected — Neo4j is already in sync.")
            written = 0
        else:
            written = apply_sync(driver, sf_df, sync_ts)
            log.info(f"[OK] Synced {written} columns to Neo4j.")

    finally:
        driver.close()

    summary = {
        "sync_ts":     sync_ts,
        "sf_columns":  len(sf_df),
        "neo4j_nodes": len(neo4j_map),
        "new":         len(new_cols),
        "changed":     len(changed),
        "unchanged":   len(unchanged),
        "written":     written if not dry_run else 0,
    }
    log.info(f"[DONE] {summary}")
    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Sync Snowflake column tags to Neo4j (incremental, no data nodes touched)."
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing anything to Neo4j.",
    )
    p.add_argument(
        "--watch",
        type=int,
        metavar="MINUTES",
        help="Re-run sync every N minutes until interrupted (Ctrl+C).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.watch:
        log.info(f"Watch mode: syncing every {args.watch} minute(s). Press Ctrl+C to stop.")
        try:
            while True:
                run_sync(dry_run=args.dry_run)
                log.info(f"Next sync in {args.watch} minute(s)...")
                time.sleep(args.watch * 60)
        except KeyboardInterrupt:
            log.info("Watch mode stopped.")
    else:
        result = run_sync(dry_run=args.dry_run)
        sys.exit(0 if result["written"] >= 0 else 1)
