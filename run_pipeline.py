import argparse
import os
import subprocess
import sys
from pathlib import Path
from itertools import zip_longest

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase


KCI_DIR = Path(__file__).resolve().parent
load_dotenv(KCI_DIR / ".env")


def run_step(title, filename, *options):
    print(f"\n===== {title} =====", flush=True)

    result = subprocess.run(
        [sys.executable, str(KCI_DIR / filename), *options],
        cwd=KCI_DIR,
    )

    if result.returncode != 0:
        raise SystemExit(
            f"{title} 실패: 종료 코드 {result.returncode}"
        )


def graph_state():
    def pg_id_stream(cursor):
        while True:
            rows = cursor.fetchmany(10_000)
            if not rows:
                return

            for row in rows:
                yield str(row[0])

    def matches_snapshot(connection, session, snapshot):
        with connection.cursor(name=f"state_{snapshot}") as cursor:
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
            matches = True

            for pg_id, graph_record in zip_longest(
                pg_id_stream(cursor),
                graph_result,
                fillvalue=sentinel,
            ):
                if pg_id is sentinel or graph_record is sentinel:
                    matches = False
                    break

                if pg_id != str(graph_record["nf_id"]):
                    matches = False
                    break

            # 불일치가 일찍 발견돼도 Neo4j 결과 스트림을 정리합니다.
            graph_result.consume()
            return matches

    with psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=5,
    ) as connection:
        with GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(
                os.getenv("NEO4J_USER"),
                os.getenv("NEO4J_PASSWORD"),
            ),
        ) as driver:
            with driver.session(
                database=os.getenv("NEO4J_DATABASE"),
                fetch_size=1_000,
            ) as session:
                if matches_snapshot(connection, session, "2024"):
                    return "2024 baseline"

                if matches_snapshot(connection, session, "2026"):
                    return "2026 target"

    return "partial/mixed"


def main():
    parser = argparse.ArgumentParser(
        description="POI 증분 갱신 모듈 1·2·3 통합 실행"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="2024 baseline에서 실제 증분 갱신 실행",
    )
    args = parser.parse_args()

    state = graph_state()
    print(f"현재 Neo4j 상태: {state}", flush=True)

    if args.apply:
        if state != "2024 baseline":
            raise SystemExit(
                "실행 중단: Neo4j가 2024 baseline이 아닙니다."
            )

        run_step(
            "모듈 1: 증분 테이블 생성",
            "create_inc_poi.py",
            "--apply",
        )
        run_step(
            "모듈 2: Neo4j 증분 반영",
            "apply_inc_poi.py",
            "--apply",
        )
        run_step(
            "모듈 3: 최종 정합성 검증",
            "verify_inc_poi.py",
        )
        print("\n통합 파이프라인 완료")
    else:
        run_step(
            "모듈 1 사전 점검",
            "create_inc_poi.py",
        )
        run_step(
            "모듈 2 사전 점검",
            "apply_inc_poi.py",
        )

        if state == "2026 target":
            run_step(
                "모듈 3 현재 상태 검증",
                "verify_inc_poi.py",
            )

        print("\n읽기 전용 점검 완료")


if __name__ == "__main__":
    main()