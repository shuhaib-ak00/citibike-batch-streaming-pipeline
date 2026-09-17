-- ============================================================
-- dim_rider_type — dimensi tipe pengguna
--
-- Nilainya terbatas & stabil (member / casual), jadi dibangkitkan langsung
-- sebagai model statis — tidak perlu seed CSV terpisah.
-- ============================================================

WITH types AS (
    SELECT 'member' AS member_casual,
           'Pengguna langganan tahunan (annual member)' AS description
    UNION ALL
    SELECT 'casual',
           'Pengguna harian / single ride (casual)'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} AS rider_type_key,
    member_casual,
    description
FROM types
