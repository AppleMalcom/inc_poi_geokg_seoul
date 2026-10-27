"""Reusable Cypher routines for regional incremental POI benchmark."""
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

EXCLUDED_PROPERTIES = {"geom", "change_type", "snapshot"}
KEYS = ("nodes_created", "nodes_deleted", "relationships_created", "relationships_deleted",
        "labels_added", "labels_removed", "properties_set")


from regional_benchmark.legacy_batch import prepare_active_row


def new_counter_accumulator():
    return {k:0 for k in KEYS}


def accumulate(acc, counters):
    for key in KEYS:
        acc[key] += getattr(counters, key)


def tombstone_batch(tx, nf_ids):
    return tx.run("""
        UNWIND $nf_ids AS nf_id MATCH (p:POI {nf_id:nf_id})
        OPTIONAL MATCH (p)-[r:IN]-(:adm_dong)
        WITH p, collect(r) AS rels
        FOREACH (r IN rels | DELETE r)
        REMOVE p:POI
        SET p:POITombstone, p.last_seen_snapshot='2024',
            p.absent_since_snapshot='2026'
        """, nf_ids=nf_ids).consume().counters


def upsert_batch(tx, rows):
    s1 = tx.run("""
        UNWIND $rows AS row MERGE (p:POI {nf_id:row.nf_id})
        SET p += row.properties
        SET p.location = point({longitude:row.longitude, latitude:row.latitude})
        """, rows=rows).consume().counters
    s2 = tx.run("""
        UNWIND $rows AS row MATCH (p:POI {nf_id:row.nf_id})
        OPTIONAL MATCH (p)-[r:IN]-(:adm_dong) DELETE r
        """, rows=rows).consume().counters
    s3 = tx.run("""
        UNWIND $rows AS row MATCH (p:POI {nf_id:row.nf_id})
        MATCH (a:adm_dong) WHERE a.adm_cd=row.adm_cd
        MERGE (p)-[:IN]->(a)
        """, rows=rows).consume().counters
    return [s1,s2,s3]
