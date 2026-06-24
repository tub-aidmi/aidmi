{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    COALESCE(TRIM(titel), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(vertriebsphase)) IN ('WON', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('LOST', 'VERLOREN', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFIKATION', 'QUALI', 'QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) = 'IN PRÜFUNG' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for StageName which is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN zieldatum -- YYYY-MM-DD
            WHEN zieldatum ~ '^\\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            WHEN zieldatum ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
            WHEN zieldatum ~ '^\\d{1,2}\\.\\d{1,2}\\.\\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            ELSE NULL
        END,
        '2023-01-01' -- Default for CloseDate which is NOT NULL
    ) AS "CloseDate",
    COALESCE(
        NULLIF(
            REPLACE(
                REPLACE(
                    REGEXP_REPLACE(TRIM(COALESCE(auftragswert, '')), '[^0-9,.\\-]+', '', 'g'),
                '.', ''),
            ',', '.'),
        ''),
    '0')::DOUBLE PRECISION AS "Amount",
    CASE
        WHEN UPPER(TRIM(waehrungscode)) = 'DOLLAR' THEN 'USD'
        WHEN UPPER(TRIM(waehrungscode)) = '€' THEN 'EUR'
        ELSE UPPER(TRIM(waehrungscode))
    END AS "CurrencyIsoCode",
    TRIM(kunden_ref) AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_opportunities') }}
