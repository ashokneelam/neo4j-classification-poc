"""
neo4j_export_snapshot.py
------------------------
Exports a full snapshot of the Neo4j graph to JSON and CSV files.

Output files (written to ./neo4j_snapshot/):
  - nodes.json          all nodes with labels and properties
  - relationships.json  all relationships with type and properties
  - summary.txt         counts by label / relationship type
  - nodes_<LABEL>.csv   one CSV per node label (for spreadsheet review)

Usage:
  pip install neo4j python-dotenv
  python neo4j_export_snapshot.py
"""

import json
import csv
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ── Load .env ────────────────────────────────────────────────────────────────
# Walk up from this file's directory to find .env
_here = Path(__file__).resolve().parent
for _candidate in [_here, _here.parent, _here.parent.parent]:
    if (_candidate / ".env").exists():
        load_dotenv(_candidate / ".env")
        break

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
    raise EnvironmentError("NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD not found in .env")

# ── Output directory ─────────────────────────────────────────────────────────
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = Path(__file__).parent / f"neo4j_snapshot_{TIMESTAMP}"
OUTPUT_DIR.mkdir(exist_ok=True)


def export_snapshot():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        print(f"Connected to {NEO4J_URI}")
        print(f"Writing snapshot to: {OUTPUT_DIR}\n")

        # ── 1. Export all nodes ───────────────────────────────────────────────
        print("Exporting nodes...")
        result = session.run("""
            MATCH (n)
            RETURN
                id(n)          AS neo4j_id,
                labels(n)      AS labels,
                properties(n)  AS props
            ORDER BY labels(n), id(n)
        """)
        nodes = [{"neo4j_id": r["neo4j_id"],
                  "labels":   r["labels"],
                  "properties": r["props"]} for r in result]

        with open(OUTPUT_DIR / "nodes.json", "w", encoding="utf-8") as f:
            json.dump(nodes, f, indent=2, default=str)
        print(f"  → {len(nodes)} nodes written to nodes.json")

        # ── 2. Export all relationships ───────────────────────────────────────
        print("Exporting relationships...")
        result = session.run("""
            MATCH (a)-[r]->(b)
            RETURN
                id(r)            AS neo4j_id,
                type(r)          AS type,
                id(a)            AS from_id,
                labels(a)        AS from_labels,
                id(b)            AS to_id,
                labels(b)        AS to_labels,
                properties(r)    AS props
            ORDER BY type(r), id(r)
        """)
        rels = [{"neo4j_id":    r["neo4j_id"],
                 "type":        r["type"],
                 "from_id":     r["from_id"],
                 "from_labels": r["from_labels"],
                 "to_id":       r["to_id"],
                 "to_labels":   r["to_labels"],
                 "properties":  r["props"]} for r in result]

        with open(OUTPUT_DIR / "relationships.json", "w", encoding="utf-8") as f:
            json.dump(rels, f, indent=2, default=str)
        print(f"  → {len(rels)} relationships written to relationships.json")

        # ── 3. Per-label CSV files ────────────────────────────────────────────
        print("Exporting per-label CSVs...")
        label_result = session.run("CALL db.labels() YIELD label RETURN label ORDER BY label")
        all_labels = [r["label"] for r in label_result]

        for label in all_labels:
            result = session.run(f"""
                MATCH (n:`{label}`)
                RETURN id(n) AS neo4j_id, properties(n) AS props
                ORDER BY id(n)
            """)
            rows = [{"neo4j_id": r["neo4j_id"], **r["props"]} for r in result]
            if not rows:
                continue

            # Collect all property keys across all nodes of this label
            all_keys = ["neo4j_id"] + sorted({k for row in rows for k in row if k != "neo4j_id"})
            csv_path = OUTPUT_DIR / f"nodes_{label}.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"  → {label}: {len(rows)} rows → nodes_{label}.csv")

        # ── 4. Summary ────────────────────────────────────────────────────────
        print("Writing summary...")

        label_counts_result = session.run("""
            MATCH (n)
            UNWIND labels(n) AS label
            RETURN label, count(*) AS cnt
            ORDER BY cnt DESC
        """)
        label_counts = [(r["label"], r["cnt"]) for r in label_counts_result]

        rel_counts_result = session.run("""
            MATCH ()-[r]->()
            RETURN type(r) AS type, count(*) AS cnt
            ORDER BY cnt DESC
        """)
        rel_counts = [(r["type"], r["cnt"]) for r in rel_counts_result]

        summary_lines = [
            f"Neo4j Snapshot — {TIMESTAMP}",
            f"URI: {NEO4J_URI}",
            "",
            f"NODES ({len(nodes)} total)",
            *[f"  {label:<30} {cnt}" for label, cnt in label_counts],
            "",
            f"RELATIONSHIPS ({len(rels)} total)",
            *[f"  {rtype:<30} {cnt}" for rtype, cnt in rel_counts],
        ]
        summary_text = "\n".join(summary_lines)
        with open(OUTPUT_DIR / "summary.txt", "w", encoding="utf-8") as f:
            f.write(summary_text)

        print("\n" + "─" * 50)
        print(summary_text)
        print("─" * 50)
        print(f"\nSnapshot complete: {OUTPUT_DIR}")

    driver.close()


if __name__ == "__main__":
    export_snapshot()
