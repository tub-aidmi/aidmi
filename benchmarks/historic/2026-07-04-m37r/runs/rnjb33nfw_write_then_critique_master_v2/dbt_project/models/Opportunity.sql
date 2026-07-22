{{ config(materialized='table') }}

SELECT
    TRIM(opps.opp_kennung) AS "Id",
    COALESCE(TRIM(opps.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('won', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('lost', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opps.vertriebsphase)) IN ('qualifikation', 'quali', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(opps.vertriebsphase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(opps.vertriebsphase)) = 'in prüfung' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opps.vertriebsphase)) = 'in kontakt' THEN 'Prospecting' -- Assuming "in kontakt" maps to Prospecting
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN opps.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(opps.zieldatum, 'YYYY-MM-DD')
                WHEN opps.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(opps.zieldatum, 'DD.MM.YYYY')
                WHEN opps.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(opps.zieldatum, 'MM/DD/YYYY')
                WHEN opps.zieldatum ~ '^\d{8}$' THEN TO_DATE(opps.zieldatum, 'YYYYMMDD')
                ELSE NULL -- Prefer NULL over sentinel dates if unparseable
            END,
            'YYYY-MM-DD'
        ),
        '1900-01-01' -- Fallback for NOT NULL CloseDate if all parsing fails or source is NULL
    ) AS "CloseDate",
    NULLIF(
        REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRIM(opps.auftragswert),
                    '[^0-9\.\,-]+', -- Remove non-numeric/non-dot/non-comma/non-minus
                    '',
                    'g'
                ),
                '\.(?=\d{3}(?:,|$))', -- Remove thousand separators (dots followed by 3 digits and comma/end)
                '',
                'g'
            ),
            ',', '.' -- Replace comma with dot for decimal
        ),
        '' -- If result is an empty string after cleanup, treat as NULL
    )::DOUBLE PRECISION AS "Amount",
    CASE
        WHEN LOWER(TRIM(opps.waehrungscode)) IN ('euro', 'eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(opps.waehrungscode)) IN ('usd', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(opps.waehrungscode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(opps.waehrungscode)) IN ('chf') THEN 'CHF'
        ELSE UPPER(TRIM(opps.waehrungscode))
    END AS "CurrencyIsoCode",
    TRIM(opps.kunden_ref) AS "AccountId",
    TRIM(opps.opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opps
