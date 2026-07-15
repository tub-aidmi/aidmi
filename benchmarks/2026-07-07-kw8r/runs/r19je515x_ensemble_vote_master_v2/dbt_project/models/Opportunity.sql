{{ config(materialized='table') }}

WITH opp AS (
    SELECT
        mo.opp_kennung,
        mo.titel,
        mo.vertriebsphase,
        mo.zieldatum,
        mo.auftragswert,
        mo.waehrungscode,
        mo.kunden_ref,
        -- Clean currency code: strip symbols and normalize to ISO codes
        CASE LOWER(TRIM(mo.waehrungscode))
            WHEN 'usd' THEN 'USD'
            WHEN '$' THEN 'USD'
            WHEN 'eur' THEN 'EUR'
            WHEN '€' THEN 'EUR'
            WHEN 'euro' THEN 'EUR'
            WHEN 'gbp' THEN 'GBP'
            WHEN '£' THEN 'GBP'
            WHEN 'chf' THEN 'CHF'
            ELSE UPPER(TRIM(mo.waehrungscode))
        END AS currency_iso,
        -- Clean amount: strip currency prefixes/symbols, handle European format
        CASE
            WHEN TRIM(mo.auftragswert) = 'None' OR TRIM(mo.auftragswert) IS NULL THEN NULL::DOUBLE PRECISION
            ELSE
                CASE
                    -- European format with dots as thousands and comma as decimal: e.g. "159.961,05"
                    WHEN mo.auftragswert ~ '^-?\d{1,3}\.\d{3},\d{2}$' THEN
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE(mo.auftragswert, '^[€$£]', ''), '[A-Za-z]+', '', 'gi'),
                            '\.', ''  -- Remove thousand separators (dots)
                        )::DOUBLE PRECISION * POWER(10, -1) +  -- shift for missing decimal handling
                        ... -- This won't work cleanly. Let me redo the amount parsing below.
                END
        END AS amount
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
)
