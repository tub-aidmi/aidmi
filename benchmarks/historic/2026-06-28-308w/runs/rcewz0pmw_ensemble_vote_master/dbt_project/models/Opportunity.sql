
{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        mo.waehrungscode,
        mo.kunden_ref,
        TRIM(REGEXP_REPLACE(mo.auftragswert, '[^0-9.,]+', '', 'g')) AS cleaned_amount_str
    FROM
        {{ source('fixture_master_src', 'master_opportunities') }} AS mo
)
SELECT
    cd.opp_kennung AS "Id",
    COALESCE(cd.titel, cd.opp_kennung) AS "Name",
    CASE
        WHEN LOWER(cd.vertriebsphase) = 'gewonnen' THEN 'Closed Won'
        WHEN LOWER(cd.vertriebsphase) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(cd.vertriebsphase) IN ('qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(cd.vertriebsphase) IN ('lost', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(cd.vertriebsphase) = 'in kontakt' THEN 'Perception Analysis'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        CASE
            WHEN cd.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN cd.zieldatum
            WHEN cd.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(cd.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN cd.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(cd.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        CURRENT_DATE::TEXT -- Fallback for unparseable/N/A dates for a NOT NULL target
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN cd.cleaned_amount_str IS NULL OR cd.cleaned_amount_str = '' THEN NULL
            ELSE
                CASE
                    -- European format: comma as decimal separator, period as thousands separator
                    -- E.g., "1.234,56", "123,45"
                    WHEN (POSITION(''',''' IN cd.cleaned_amount_str) > 0 AND POSITION('.' IN cd.cleaned_amount_str) > 0 AND POSITION(''',''' IN cd.cleaned_amount_str) > POSITION('.' IN cd.cleaned_amount_str))
                         OR (POSITION(''',''' IN cd.cleaned_amount_str) > 0 AND POSITION('.' IN cd.cleaned_amount_str) = 0)
                    THEN
                        REPLACE(REPLACE(cd.cleaned_amount_str, '.', ''), ',