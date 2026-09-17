-- ============================================================
-- assert_rejected_pct_below_threshold
--
-- Bila proporsi baris karantina melewati threshold (default 5%) untuk sebuah
-- file sumber, itu sinyal masalah SISTEMIK (skema sumber berubah / bug
-- upstream), bukan noise — pipeline harus gagal agar alert terpicu.
--
-- Gagal bila query mengembalikan baris.
-- ============================================================

WITH per_file AS (
    SELECT
        _source_file,
        SUM(row_count)      AS total_rows,
        SUM(rejected_rows)  AS rejected_rows,
        ROUND(SAFE_DIVIDE(SUM(rejected_rows), SUM(row_count)) * 100, 4) AS rejected_pct
    FROM {{ ref('int_trips_dq_summary') }}
    GROUP BY _source_file
)

SELECT
    _source_file,
    total_rows,
    rejected_rows,
    rejected_pct,
    {{ var('quarantine_alert_threshold_pct') }} AS threshold_pct
FROM per_file
WHERE rejected_pct > {{ var('quarantine_alert_threshold_pct') }}
