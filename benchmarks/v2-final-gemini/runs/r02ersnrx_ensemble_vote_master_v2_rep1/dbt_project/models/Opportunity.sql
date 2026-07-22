{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung) AS "Id",
    COALESCE(o.titel, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.vertriebsphase)) = 'in kontakt' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('abgeschlossen (gewonnen)', 'closed won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum::DATE
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.zieldatum, 'DD.MM.YYYY')
            WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(o.zieldatum, 'MM/DD/YYYY')
            WHEN o.zieldatum ~ '^\d{8}$' THEN TO_DATE(o.zieldatum, 'YYYYMMDD')
            ELSE NULL
        END,
        CURRENT_DATE
    )::TEXT AS "CloseDate", -- Target expects TEXT YYYY-MM-DD
    CASE
        WHEN o.auftragswert IS NULL OR TRIM(o.auftragswert) = '' THEN NULL
        WHEN REGEXP_REPLACE(REPLACE(TRIM(o.auftragswert), ',', ''), '[^0-9\.-]', '', 'g') ~ '^-?\d+\.?\d*$'
            THEN CAST(REGEXP_REPLACE(REPLACE(TRIM(o.auftragswert), ',', ''), '[^0-9\.-]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE LOWER(TRIM(o.waehrungscode))
        WHEN 'chf' THEN 'CHF'
        WHEN 'eur' THEN 'EUR'
        WHEN 'euro' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'usd' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN 'gbp' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    MD5(o.kunden_ref) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} o