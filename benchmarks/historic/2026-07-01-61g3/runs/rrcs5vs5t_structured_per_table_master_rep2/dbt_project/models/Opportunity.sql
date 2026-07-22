{{ config(materialized='table') }}

SELECT
    CAST(m.opponent_kennung AS text) AS "Id",
    COALESCE(NULLIF(TRIM(m.titel), ''), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(m.vertriebsphase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'in kontakt' THEN 'Needs Analysis'
        WHEN 'in prüfung' THEN 'Value Proposition'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN m.zieldatum IS NULL OR TRIM(m.zieldatum) = '' OR TRIM(UPPER(m.zieldatum)) = 'N/A' OR TRIM(m.zieldatum) = '0000-00-00' THEN NULL
        WHEN m.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(m.zieldatum), 'YYYY-MM-DD')::text
        WHEN m.zieldatum ~ '^\d{8}$' THEN TO_DATE(TRIM(m.zieldatum), 'YYYYMMDD')::text
        WHEN m.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(m.zieldatum), 'DD.MM.YYYY')::text
        WHEN m.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(m.zieldatum), 'MM/DD/YYYY')::text
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN m.auftragswert IS NULL OR TRIM(m.auftragswert) = '' OR UPPER(TRIM(m.auftragswert)) = 'NONE' THEN NULL
        ELSE
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REPLACE(TRIM(m.auftragswert), 'EUR ', ''),
                    '\.', ''  -- Remove thousand-separator dots (European format)
                ),
                ',', '.'  -- Replace decimal comma with period
            )::DOUBLE PRECISION
    END AS "Amount",
    CASE UPPER(TRIM(COALESCE(m.waehrungscode, '')))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '€' THEN 'EUR'
        ELSE NULL
    END AS "CurrencyIsoCode",
    REPLACE(TRIM(k.kundennummer), '', '') AS "AccountId",
    m.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }} m
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} k
    ON REPLACE(TRIM(m.kunden_ref), 'KD-M', 'CUST-M') = TRIM(k.kundennummer)