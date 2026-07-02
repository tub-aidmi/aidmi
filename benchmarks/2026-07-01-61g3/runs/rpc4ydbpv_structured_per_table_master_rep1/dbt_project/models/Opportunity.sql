{{ config(materialized='table') }}

SELECT
    opp."opp_kennung" AS "Id",
    COALESCE(opp."titel", 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(opp."vertriebsphase")) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(opp."vertriebsphase")) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(opp."vertriebsphase")) = 'IN KONTAKT' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp."vertriebsphase")) = 'IN PRÜFUNG' THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp."vertriebsphase")) IN ('GEWONNEN', 'WON', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(opp."vertriebsphase")) IN ('VERLOREN', 'LOST', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp."zieldatum" IS NULL OR TRIM(opp."zieldatum") = '' OR TRIM(opp."zieldatum") = 'N/A' THEN NULL
        WHEN TRIM(opp."zieldatum") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(opp."zieldatum"), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(opp."zieldatum") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(opp."zieldatum"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(opp."zieldatum") ~ '^\d{8}$' THEN TO_DATE(TRIM(opp."zieldatum"), 'YYYYMMDD')::TEXT
        WHEN TRIM(opp."zieldatum") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(opp."zieldatum"), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp."auftragswert" IS NULL 
             OR TRIM(opp."auftragswert") = '' 
             OR UPPER(TRIM(opp."auftragswert")) = 'NONE' THEN NULL
        ELSE CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(opp."auftragswert", '^\s*EUR\s*', '', 'i'),
                        '^\s*\$?\s*', '', 'i'),
                    '^€', ''),
            '\\.', '')::DOUBLE PRECISION
    END AS "Amount",
    CASE
        WHEN opp."waehrungscode" IS NULL OR TRIM(opp."waehrungscode") = '' THEN NULL
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM(opp."waehrungscode")) = 'CHF' THEN 'CHF'
        ELSE UPPER(TRIM(opp."waehrungscode"))
    END AS "CurrencyIsoCode",
    C.Kundennummer AS "AccountId",
    opp."opp_kennung" AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }} opp
LEFT JOIN (
    SELECT 
        "kundennummer",
        REPLACE("kundennummer", 'CUST-M', 'KD-M') AS legacy_key
    FROM {{ source('fixture_master_src', 'master_kunden') }}
) C ON REGEXP_REPLACE(opp."kunden_ref", '_DUP$', '') = C.legacy_key

WHERE opp."opp_kennung" IS NOT NULL