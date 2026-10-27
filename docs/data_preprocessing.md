# 데이터 전처리 및 실험 DB 구성 기록

## 1. 전처리 목적

- 국가 POI의 2024년·2026년 snapshot을 동일한 서울특별시 공간 범위로 정규화한다.
- POI와 행정동의 공간 포함 관계를 PostGIS에서 사전 계산한다.
- Neo4j 갱신 시 반복적인 공간 연산 없이 `adm_cd`를 이용해 관계를 생성한다.
- 두 snapshot을 같은 테이블 구조로 관리하여 SQL 기반 CDC의 입력으로 사용한다.

## 2. 원천 데이터

- `raw.tn_poi_2024`
  - 국가 POI 2024년 11월 snapshot
  - 1,905,940건
  - 좌표계: EPSG:5179
- `raw.tn_poi_2026`
  - 국가 POI 2026년 1월 snapshot
  - 1,574,289건
  - 좌표계: EPSG:5179
- `raw.bnd_sido_pg`
  - SGIS 2025년 시도 경계
- `raw.bnd_sigungu_pg`
  - SGIS 2025년 시군구 경계
- `raw.bnd_adm_dong_pg`
  - SGIS 2025년 행정동 경계
- 원천 행정경계 좌표계: EPSG:5186
- `raw.tn_alias`, `raw.tn_adres`는 본 연구에서 사용하지 않는다.
- 기존 `raw.tn_asortcl`은 `mart.poi_category`로 정리해 보존한다.

## 3. 행정경계 전처리

- 행정구역 코드가 `11`로 시작하는 객체만 추출한다.
- 원천 경계를 EPSG:5186에서 EPSG:5179로 변환한다.
- 코드 컬럼을 PK로 지정하고 geometry에 GiST 인덱스를 생성한다.
- 생성 테이블
  - `mart.bnd_seoul_sido`: 서울특별시 1건
  - `mart.bnd_seoul_sigungu`: 서울시 자치구 25건
  - `mart.bnd_seoul_adm_dong`: 서울시 행정동 426건
- 두 POI snapshot에 동일한 2025년 행정경계를 적용하여 경계 버전 차이가 POI 변화로 판정되는 것을 방지한다.

## 4. 서울시 POI 추출

- 서울특별시 polygon과 POI point의 공간 포함 관계로 대상 범위를 결정한다.
- 경계선 위의 POI도 포함하기 위해 `ST_Covers`를 사용한다.
- bounding-box 연산자 `&&`를 함께 사용하여 GiST 공간 인덱스를 활용한다.
- 행정구역 명칭이나 원천 주소 문자열은 서울시 추출 조건으로 사용하지 않는다.

## 5. 행정동 할당

- 각 POI point와 `mart.bnd_seoul_adm_dong` polygon을 공간 결합한다.
- `ST_Contains`에 해당하는 내부 point를 우선한다.
- 공유 경계선 위의 point는 `adm_cd` 순서로 하나의 행정동을 결정하여 결과의 재현성을 확보한다.
- `adm_cd`는 Neo4j에서 POI–행정동 관계를 생성하는 식별 키로 사용한다.
- `adm_nm`은 적재 결과 확인과 오류 검증을 위한 보조 컬럼으로 저장한다.

## 6. POI 좌표 처리

- PostGIS 분석용 `geom`은 EPSG:5179로 유지한다.
- Neo4j의 WGS-84 `point` 생성을 위해 EPSG:4326으로 변환한 `longitude`, `latitude`를 함께 저장한다.
- Neo4j에는 PostGIS `geometry` 객체를 직접 저장하지 않고 다음 구조로 변환한다.

```cypher
point({longitude: row.longitude, latitude: row.latitude})
```

## 7. mart.poi 관리 구조

- PK: `(snapshot, nf_id)`
- 파티션 방식: `snapshot` 기준 LIST 파티션
- 파티션
  - `mart.poi_snapshot_2024`
  - `mart.poi_snapshot_2026`
- 동일한 `nf_id`가 여러 snapshot에 존재할 수 있으므로 복합 PK를 사용한다.
- 기존 서초구 학술대회 실험 데이터는 `mart.poi_seocho`로 별도 보존한다.

## 8. 적재 결과

- 2024 snapshot
  - 서울시 POI: 1,905,592건
  - raw 대비 경계 밖 제외: 348건
- 2026 snapshot
  - 서울시 POI: 1,574,276건
  - raw 대비 경계 밖 제외: 13건
- snapshot 내부 `nf_id` 중복: 0건
- 행정동 미매칭: 0건
- geometry 결측: 0건

## 9. 후속 처리

- `mart.poi`의 두 snapshot을 `nf_id`로 연결한다.
- `poi_nm`, `poi_cl_dc`, `adm_cd`, `geom` 등 감시 속성을 비교한다.
- 공간 변화는 EPSG:5179에서 0.1m를 초과할 때 수정으로 판정한다.
- 탐지 결과를 `mart.inc_poi`에 저장한다.
- `mart.inc_poi`를 이용해 기존 Neo4j GeoKG를 증분 갱신한다.
- PostgreSQL과 Neo4j의 현재 상태, 속성 및 행정동 관계를 검증한다.

