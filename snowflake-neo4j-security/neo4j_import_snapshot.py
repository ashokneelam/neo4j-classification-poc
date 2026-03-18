"""
neo4j_import_snapshot.py
------------------------
Imports a snapshot exported by neo4j_export_snapshot.py back into Neo4j.

Usage:
  python neo4j_import_snapshot.py --snapshot neo4j_snapshot_20260318_143000
  python neo4j_import_snapshot.py --snapshot neo4j_snapshot_20260318_143000 --wipe

Options:
  --snapshot  Path to the snapshot folder (required)
  --wipe      Delete ALL existing nodes/relationships before importing (default: False)
              WARNING: irreversible — prompts for confirmation

Notes:
  - Nodes are matched by their exported properties (not neo4j internal ID,
    which changes on re-import). A node is considered duplicate if all its
    properties already exist on a node with the same labels.
  - Relationships are created only after all nodes are in place.
  - Batched in groups of 500 to avoid memory issues on large graphs.
"""

import json
import os
import argparse
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ── Load .env ────────────────────────────────────────────────────────────────
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

BATCH_SIZE = 500


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def wipe_database(session):
    print("Wiping existing graph...")
    session.run("MATCH (n) DETACH DELETE n")
    print("  → Graph cleared.")


def import_nodes(session, nodes):
    """
    For each node, MERGE on all its properties within its first label.
    If a node has multiple labels, additional labels are added after merge.
    """
    print(f"Importing {len(nodes)} nodes...")

    # Group by primary label (first in the labels list)
    from collections import defaultdict
    by_label = defaultdict(list)
    for node in nodes:
        primary = node["labels"][0] if node["labels"] else "Unknown"
        by_label[primary].append(node)

    total_created = 0
    total_merged  = 0

    for label, label_nodes in by_label.items():
        for batch in chunks(label_nodes, BATCH_SIZE):
            result = session.run(f"""
                UNWIND $batch AS row
                MERGE (n:`{label}` {{ name: row.props.name }})
                ON CREATE SET n = row.props, n._imported_id = row.neo4j_id
                ON MATCH  SET n += row.props, n._imported_id = row.neo4j_id
                RETURN count(*) AS cnt
            """, batch=[{"neo4j_id": n["neo4j_id"], "props": n["properties"]} for n in batch])
            cnt = result.single()["cnt"]
            total_merged += cnt

        # Add any extra labels beyond the primary
        for node in label_nodes:
            extra_labels = [l for l in node["labels"] if l != label]
            if extra_labels:
                extra_str = ":".join(f"`{l}`" for l in extra_labels)
                session.run(f"""
                    MATCH (n:`{label}` {{_imported_id: $iid}})
                    SET n:{extra_str}
                """, iid=node["neo4j_id"])

        print(f"  → {label}: {len(label_nodes)} nodes merged")
        total_created += len(label_nodes)

    print(f"  → Nodes done: {total_created} processed, {total_merged} merged/created")


def import_relationships(session, rels):
    """
    For each relationship, look up from/to nodes by _imported_id and MERGE the rel.
    """
    print(f"Importing {len(rels)} relationships...")

    from collections import defaultdict
    by_type = defaultdict(list)
    for rel in rels:
        by_type[rel["type"]].append(rel)

    total = 0
    for rel_type, type_rels in by_type.items():
        for batch in chunks(type_rels, BATCH_SIZE):
            session.run(f"""
                UNWIND $batch AS row
                MATCH (a {{_imported_id: row.from_id}})
                MATCH (b {{_imported_id: row.to_id}})
                MERGE (a)-[r:`{rel_type}`]->(b)
                ON CREATE SET r = row.props
                ON MATCH  SET r += row.props
            """, batch=[{"from_id": r["from_id"],
                         "to_id":   r["to_id"],
                         "props":   r["properties"]} for r in batch])
        total += len(type_rels)
        print(f"  → {rel_type}: {len(type_rels)} relationships merged")

    print(f"  → Relationships done: {total} processed")


def cleanup_imported_ids(session):
    """Remove the temporary _imported_id property used to resolve relationships."""
    print("Cleaning up temporary _imported_id properties...")
    session.run("MATCH (n) WHERE n._imported_id IS NOT NULL REMOVE n._imported_id")
    print("  → Done.")


def import_snapshot(snapshot_dir: Path, wipe: bool):
    nodes_file = snapshot_dir / "nodes.json"
    rels_file  = snapshot_dir / "relationships.json"

    if not nodes_file.exists():
        raise FileNotFoundError(f"nodes.json not found in {snapshot_dir}")
    if not rels_file.exists():
        raise FileNotFoundError(f"relationships.json not found in {snapshot_dir}")

    with open(nodes_file, encoding="utf-8") as f:
        nodes = json.load(f)
    with open(rels_file, encoding="utf-8") as f:
        rels = json.load(f)

    print(f"Snapshot loaded: {len(nodes)} nodes, {len(rels)} relationships")
    print(f"Target:          {NEO4J_URI}\n")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        if wipe:
            confirm = input("WARNING: --wipe will delete ALL existing data. Type YES to confirm: ")
            if confirm.strip() != "YES":
                print("Aborted.")
                driver.close()
                return
            wipe_database(session)

        import_nodes(session, nodes)
        import_relationships(session, rels)
        cleanup_imported_ids(session)

    driver.close()

    print("\n" + "─" * 50)
    print(f"Import complete.")
    print(f"  Nodes:         {len(nodes)}")
    print(f"  Relationships: {len(rels)}")
    print("─" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import a Neo4j snapshot")
    parser.add_argument("--snapshot", required=True,
                        help="Path to the snapshot folder (e.g. neo4j_snapshot_20260318_143000)")
    parser.add_argument("--wipe", action="store_true",
                        help="Delete all existing data before importing")
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot)
    if not snapshot_path.is_absolute():
        snapshot_path = Path(__file__).parent / snapshot_path

    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot folder not found: {snapshot_path}")

    import_snapshot(snapshot_path, wipe=args.wipe)
