import argparse
import os
import time
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from itertools import zip_longest

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from neo4j import GraphDatabase


KCI_DIR = Path(__file__).resolve().parent
load_dotenv(KCI_DIR / ".env")

# PostgreSQL에는 존재하지만 Neo4j 속성으로 직접 넣지 않을 컬럼
EXCLUDED_PROPERTIES = {
    "geom",         # PostGIS Geometry
    "change_type",  # CDC 처리용 메타데이터
    "snapshot",     # Neo4j 기존 모델에는 사용하지 않음
}


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "mart.inc_poi를 기존 Neo4j GeoKG에 "
            "배치 단위로 반영합니다."
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 Neo4j 변경을 수행합니다.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Neo4j 트랜잭션당 처리 건수",
    )

    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError(
            "batch-size는 1 이상이어야 합니다."
        )

    return args


def connect_postgresql():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=5,
        row_factory=dict_row,
    )


def connect_neo4j():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(
            os.getenv("NEO4J_USER"),
            os.getenv("NEO4J_PASSWORD"),
        ),
    )

VALID_CHANGE_TYPES = {"appear", "disappear", "updated"}

def validate_inc_poi(pg_conn):
    counts = pg_conn.execute(
        """
        SELECT
            change_type,
            COUNT(*) AS row_count
        FROM mart.inc_poi
        GROUP BY change_type
        ORDER BY change_type
        """
    ).fetchall()

    if not counts:
        raise RuntimeError(
            "mart.inc_poi가 비어 있습니다."
        )

    detected_types = {
        row["change_type"] for row in counts
    }

    unknown_types = (
        detected_types - VALID_CHANGE_TYPES
    )

    if unknown_types:
        raise RuntimeError(
            f"알 수 없는 변화 유형: {unknown_types}"
        )

    total_count = sum(
        row["row_count"] for row in counts
    )

    print("PostgreSQL 증분 데이터:")

    for row in counts:
        print(
            f"- {row['change_type']}: "
            f"{row['row_count']:,}"
        )

    print(f"- 전체: {total_count:,}")

    invalid_count = pg_conn.execute(
        """
        SELECT COUNT(*) AS invalid_count
        FROM mart.inc_poi
        WHERE nf_id IS NULL
           OR change_type IS NULL
           OR (
               change_type IN ('appear', 'updated')
               AND (
                   adm_cd IS NULL
                   OR longitude IS NULL
                   OR latitude IS NULL
               )
           )
        """
    ).fetchone()["invalid_count"]

    if invalid_count:
        raise RuntimeError(
            "필수값이 누락된 증분 데이터: "
            f"{invalid_count:,}건"
        )

    duplicate_count = pg_conn.execute(
        """
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT nf_id
            FROM mart.inc_poi
            GROUP BY nf_id
            HAVING COUNT(*) > 1
        ) AS duplicated
        """
    ).fetchone()["duplicate_count"]

    if duplicate_count:
        raise RuntimeError(
            f"중복 NF_ID 그룹: {duplicate_count:,}개"
        )

    print("mart.inc_poi 사전 검증 성공")

    return {
        row["change_type"]: row["row_count"]
        for row in counts
    }

def ids_match_snapshot(pg_conn, session, snapshot):
    def pg_id_stream(cursor):
        while True:
            rows = cursor.fetchmany(10_000)
            if not rows:
                return

            for row in rows:
                yield str(row["nf_id"])

    with pg_conn.cursor(name=f"compare_{snapshot}") as cursor:
        cursor.execute(
            """
            SELECT nf_id::text
            FROM mart.poi
            WHERE snapshot = %s
            ORDER BY nf_id::text COLLATE "C"
            """,
            (snapshot,),
        )

        graph_result = session.run(
            """
            MATCH (p:POI)
            RETURN toString(p.nf_id) AS nf_id
            ORDER BY nf_id
            """
        )

        sentinel = object()

        for pg_id, graph_record in zip_longest(
            pg_id_stream(cursor),
            graph_result,
            fillvalue=sentinel,
        ):
            if pg_id is sentinel or graph_record is sentinel:
                graph_result.consume()
                return False

            if pg_id != str(graph_record["nf_id"]):
                graph_result.consume()
                return False

        graph_result.consume()
        return True

def validate_neo4j_structure(pg_conn, session):
    constraint_count = session.run(
        """
        SHOW CONSTRAINTS
        YIELD labelsOrTypes, properties
        WHERE 'POI' IN labelsOrTypes
          AND 'nf_id' IN properties
        RETURN count(*) AS constraint_count
        """
    ).single()["constraint_count"]

    if constraint_count == 0:
        raise RuntimeError(
            "POI.nf_id 유일성 제약조건이 없습니다."
        )

    graph_count = session.run(
        "MATCH (p:POI) RETURN count(p) AS count"
    ).single()["count"]

    if ids_match_snapshot(pg_conn, session, "2024"):
        state = "2024 baseline"
    elif ids_match_snapshot(pg_conn, session, "2026"):
        state = "2026 target"
    else:
        state = "partial/mixed"

    print("현재 Neo4j 상태:", state)
    print("현재 POI 노드:", f"{graph_count:,}")
    print("POI NF_ID 제약조건 확인 성공")

    required_adm_codes = {
        str(row["adm_cd"])
        for row in pg_conn.execute(
            """
            SELECT DISTINCT adm_cd
            FROM mart.inc_poi
            WHERE change_type IN ('appear', 'updated')
            """
        ).fetchall()
    }

    adm_rows = session.run(
        """
        MATCH (a:adm_dong)
        WHERE toString(a.adm_cd) IN $codes
        RETURN toString(a.adm_cd) AS adm_cd, count(a) AS node_count
        """,
        codes=sorted(required_adm_codes),
    ).data()

    adm_counts = {
        row["adm_cd"]: row["node_count"]
        for row in adm_rows
    }

    invalid_codes = [
        code for code in required_adm_codes
        if adm_counts.get(code) != 1
    ]

    if invalid_codes:
        raise RuntimeError(
            "adm_dong가 없거나 중복된 행정동 코드: "
            f"{invalid_codes}"
        )

    print("행정동 연결 대상 검증 성공:", sorted(required_adm_codes))
    return state


def convert_value(value):
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, UUID):
        return str(value)

    if isinstance(
        value,
        (
            str,
            int,
            float,
            bool,
            bytes,
            date,
            datetime,
            datetime_time,
        ),
    ):
        return value

    return str(value)


def prepare_active_row(row):
    properties = {
        key: convert_value(value)
        for key, value in row.items()
        if key not in EXCLUDED_PROPERTIES
    }

    return {
        "nf_id": str(row["nf_id"]),
        "adm_cd": str(row["adm_cd"]),
        "longitude": float(row["longitude"]),
        "latitude": float(row["latitude"]),
        "properties": properties,
    }


def stream_batches(
    pg_conn,
    change_type,
    batch_size,
):
    cursor_name = (
        f"inc_poi_{change_type}_cursor"
    )

    with pg_conn.cursor(
        name=cursor_name
    ) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM mart.inc_poi
            WHERE change_type = %s
            ORDER BY nf_id
            """,
            (change_type,),
        )

        while True:
            rows = cursor.fetchmany(batch_size)

            if not rows:
                break

            yield rows


def tombstone_batch(tx, nf_ids):
    result = tx.run(
        """
        UNWIND $nf_ids AS nf_id
        MATCH (p:POI {nf_id: nf_id})
        OPTIONAL MATCH (p)-[r:IN]-(:adm_dong)
        WITH p, collect(r) AS old_in_relationships

        FOREACH (rel IN old_in_relationships | DELETE rel)

        REMOVE p:POI
        SET p:POITombstone,
            p.last_seen_snapshot = '2024',
            p.absent_since_snapshot = '2026'

        RETURN count(p) AS tombstoned
        """,
        nf_ids=nf_ids,
    )

    tombstoned = result.single()["tombstoned"]

    if tombstoned != len(nf_ids):
        raise RuntimeError(
            "Tombstone 전환 건수 불일치: "
            f"요청={len(nf_ids)}, 전환={tombstoned}"
        )

    return tombstoned


def upsert_batch(tx, rows):
    upsert_result = tx.run(
        """
        UNWIND $rows AS row

        MERGE (p:POI {nf_id: row.nf_id})
        SET p += row.properties
        SET p.location = point({
            longitude: row.longitude,
            latitude: row.latitude
        })

        RETURN count(p) AS processed
        """,
        rows=rows,
    )

    processed = (
        upsert_result.single()["processed"]
    )

    if processed != len(rows):
        raise RuntimeError(
            "노드 반영 건수 불일치: "
            f"요청={len(rows)}, 처리={processed}"
        )

    tx.run(
        """
        UNWIND $rows AS row

        MATCH (p:POI {nf_id: row.nf_id})
        OPTIONAL MATCH
            (p)-[old:IN]-(:adm_dong)
        DELETE old
        """,
        rows=rows,
    ).consume()

    relationship_result = tx.run(
        """
        UNWIND $rows AS row

        MATCH (p:POI {nf_id: row.nf_id})
        MATCH (a:adm_dong)
        WHERE a.adm_cd = row.adm_cd

        MERGE (p)-[r:IN]->(a)

        RETURN count(r) AS linked
        """,
        rows=rows,
    )

    linked = (
        relationship_result.single()["linked"]
    )

    if linked != len(rows):
        raise RuntimeError(
            "IN 관계 반영 건수 불일치: "
            f"요청={len(rows)}, 연결={linked}"
        )

    return processed, linked


def apply_disappear(
    pg_conn,
    session,
    batch_size,
    expected_count,
):
    processed = 0
    tombstone = 0
    start_time = time.perf_counter()

    for rows in stream_batches(
        pg_conn,
        "disappear",
        batch_size,
    ):
        nf_ids = [
            str(row["nf_id"]) for row in rows
        ]

        tombstone += session.execute_write(
            tombstone_batch,
            nf_ids,
        )

        processed += len(rows)

        print(
            f"\rdisappear 처리: "
            f"{processed:,}/{expected_count:,}",
            end="",
            flush=True,
        )

    print()

    elapsed = time.perf_counter() - start_time

    print(
        f"disappear 완료: 대상={processed:,}, "
        f"Tombstone 전환={tombstone:,}, "
        f"시간={elapsed:.2f}초"
    )


def apply_active_type(
    pg_conn,
    session,
    change_type,
    batch_size,
    expected_count,
):
    processed = 0
    linked = 0
    start_time = time.perf_counter()

    for rows in stream_batches(
        pg_conn,
        change_type,
        batch_size,
    ):
        prepared_rows = [
            prepare_active_row(row)
            for row in rows
        ]

        batch_processed, batch_linked = (
            session.execute_write(
                upsert_batch,
                prepared_rows,
            )
        )

        processed += batch_processed
        linked += batch_linked

        print(
            f"\r{change_type} 처리: "
            f"{processed:,}/{expected_count:,}",
            end="",
            flush=True,
        )

    print()

    elapsed = time.perf_counter() - start_time

    if processed != expected_count:
        raise RuntimeError(
            f"{change_type} 최종 처리 건수 불일치"
        )

    print(
        f"{change_type} 완료: "
        f"노드={processed:,}, "
        f"IN 관계={linked:,}, "
        f"시간={elapsed:.2f}초"
    )


def validate_target_state(pg_conn, session):
    if not ids_match_snapshot(pg_conn, session, "2026"):
        raise RuntimeError(
            "2026 snapshot의 NF_ID와 Neo4j POI 노드의 NF_ID가 일치하지 않습니다."
        )

    relationship_result = session.run(
        """
        MATCH (p:POI)
        OPTIONAL MATCH
            (p)-[r:IN]->(:adm_dong)

        WITH p, count(r) AS relationship_count

        RETURN
            count(p) AS poi_count,

            sum(
                CASE
                    WHEN relationship_count = 0
                    THEN 1
                    ELSE 0
                END
            ) AS without_in,

            sum(
                CASE
                    WHEN relationship_count > 1
                    THEN 1
                    ELSE 0
                END
            ) AS multiple_in
        """
    ).single()

    if (
        relationship_result["without_in"] != 0
        or relationship_result["multiple_in"] != 0
    ):
        raise RuntimeError(
            "IN 관계 구조 오류: "
            f"누락={relationship_result['without_in']}, "
            f"중복={relationship_result['multiple_in']}"
        )

    print("2026 target NF_ID 집합 일치")
    print(
        "최종 POI 노드:",
        f"{relationship_result['poi_count']:,}",
    )
    print("모든 POI의 IN 관계 1개 확인")


args = parse_arguments()

with connect_postgresql() as pg_connection:
    counts = validate_inc_poi(pg_connection)

    with connect_neo4j() as neo4j_driver:
        neo4j_driver.verify_connectivity()

        with neo4j_driver.session(
            database=os.getenv("NEO4J_DATABASE")
        ) as neo4j_session:
            state = validate_neo4j_structure(
                pg_connection,
                neo4j_session,
            )

            print("Neo4j 배치 크기:", f"{args.batch_size:,}")

            if not args.apply:
                print(
                    "실제 실행 명령: "
                    "python apply_inc_poi.py --apply"
                )
                raise SystemExit(0)

            if state != "2024 baseline":
                raise RuntimeError(
                    "증분 반영은 2024 baseline에서만 실행할 수 있습니다: "
                    f"{state}"
                )

            total_start = time.perf_counter()

            apply_disappear(
                pg_connection,
                neo4j_session,
                args.batch_size,
                counts.get("disappear", 0),
            )

            apply_active_type(
                pg_connection,
                neo4j_session,
                "appear",
                args.batch_size,
                counts.get("appear", 0),
            )

            apply_active_type(
                pg_connection,
                neo4j_session,
                "updated",
                args.batch_size,
                counts.get("updated", 0),
            )

            validate_target_state(
                pg_connection,
                neo4j_session,
            )

            total_elapsed = time.perf_counter() - total_start

            print("전체 Neo4j 반영 시간:", f"{total_elapsed:.2f}초")
            print("mart.inc_poi → Neo4j 배치 반영 완료")