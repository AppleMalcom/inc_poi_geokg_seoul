-- ============================================================
-- National POI snapshots(raw) -> Seoul POI snapshots(mart)
-- POI/target boundary CRS: EPSG:5179
-- This script recreates mart.poi. Preserve legacy data separately.
-- ============================================================

BEGIN;

DROP TABLE IF EXISTS mart.poi;

-- Copy source column types from the newer raw snapshot, then add
-- snapshot/version, WGS-84 coordinates, and administrative-dong mapping.
CREATE TABLE mart.poi (
    snapshot text NOT NULL,
    LIKE raw.tn_poi_2026
) PARTITION BY LIST (snapshot);

ALTER TABLE mart.poi
    DROP COLUMN gid,
    ADD COLUMN longitude double precision,
    ADD COLUMN latitude double precision,
    ADD COLUMN adm_cd text,
    ADD COLUMN adm_nm text,
    ADD CONSTRAINT poi_pk PRIMARY KEY (snapshot, nf_id);

CREATE TABLE mart.poi_snapshot_2024
    PARTITION OF mart.poi FOR VALUES IN ('2024');

CREATE TABLE mart.poi_snapshot_2026
    PARTITION OF mart.poi FOR VALUES IN ('2026');

CREATE INDEX poi_geom_gix
    ON mart.poi USING gist (geom);

CREATE INDEX poi_adm_cd_idx
    ON mart.poi (adm_cd);

CREATE INDEX poi_cl_dc_idx
    ON mart.poi (poi_cl_dc);

-- 2024 raw does not contain poi_refrn or mgmt_nm.
INSERT INTO mart.poi (
    snapshot, nf_id, poi_nm, poi_cl_dc, origin_se, refrn_id,
    poi_refrn, poi_geosd, mgmt_nm, dacol_dt, obchg_dt, mnent_nm,
    geom, longitude, latitude, adm_cd, adm_nm
)
SELECT
    '2024',
    p.nf_id,
    p.poi_nm,
    p.poi_cl_dc,
    p.origin_se,
    p.refrn_id,
    NULL,
    p.poi_geosd,
    NULL,
    p.dacol_dt,
    p.obchg_dt,
    p.mnent_nm,
    p.geom,
    ST_X(ST_Transform(p.geom, 4326)),
    ST_Y(ST_Transform(p.geom, 4326)),
    a.adm_cd,
    a.adm_nm
FROM raw.tn_poi_2024 AS p
JOIN mart.bnd_seoul_sido AS s
  ON s.sido_cd = '11'
 AND p.geom && s.geom
 AND ST_Covers(s.geom, p.geom)
LEFT JOIN LATERAL (
    SELECT d.adm_cd, d.adm_nm
    FROM mart.bnd_seoul_adm_dong AS d
    WHERE p.geom && d.geom
      AND ST_Covers(d.geom, p.geom)
    ORDER BY
        CASE WHEN ST_Contains(d.geom, p.geom) THEN 0 ELSE 1 END,
        d.adm_cd
    LIMIT 1
) AS a ON TRUE;

INSERT INTO mart.poi (
    snapshot, nf_id, poi_nm, poi_cl_dc, origin_se, refrn_id,
    poi_refrn, poi_geosd, mgmt_nm, dacol_dt, obchg_dt, mnent_nm,
    geom, longitude, latitude, adm_cd, adm_nm
)
SELECT
    '2026',
    p.nf_id,
    p.poi_nm,
    p.poi_cl_dc,
    p.origin_se,
    p.refrn_id,
    p.poi_refrn,
    p.poi_geosd,
    p.mgmt_nm,
    p.dacol_dt,
    p.obchg_dt,
    p.mnent_nm,
    p.geom,
    ST_X(ST_Transform(p.geom, 4326)),
    ST_Y(ST_Transform(p.geom, 4326)),
    a.adm_cd,
    a.adm_nm
FROM raw.tn_poi_2026 AS p
JOIN mart.bnd_seoul_sido AS s
  ON s.sido_cd = '11'
 AND p.geom && s.geom
 AND ST_Covers(s.geom, p.geom)
LEFT JOIN LATERAL (
    SELECT d.adm_cd, d.adm_nm
    FROM mart.bnd_seoul_adm_dong AS d
    WHERE p.geom && d.geom
      AND ST_Covers(d.geom, p.geom)
    ORDER BY
        CASE WHEN ST_Contains(d.geom, p.geom) THEN 0 ELSE 1 END,
        d.adm_cd
    LIMIT 1
) AS a ON TRUE;

ANALYZE mart.poi;

COMMIT;

-- Load validation
SELECT
    snapshot,
    count(*) AS poi_count,
    count(DISTINCT nf_id) AS distinct_nf_id,
    count(*) FILTER (WHERE adm_cd IS NULL) AS unmatched_adm_dong,
    count(*) FILTER (WHERE geom IS NULL) AS null_geometry,
    count(DISTINCT LEFT(adm_cd, 5)) AS sigungu_count,
    count(DISTINCT adm_cd) AS adm_dong_count
FROM mart.poi
GROUP BY snapshot
ORDER BY snapshot;

