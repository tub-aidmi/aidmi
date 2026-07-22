{{ config(materialized='table') }}

SELECT
    opp."opp_kennung" AS "Id",
    COALESCE(TRIM(opp."titel"), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('IN KONTAKT') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('IN PRÜFUNG') THEN 'Value Proposition'
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('GEWONNEN', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(COALESCE(opp."vertriebsphase", ''))) IN ('VERLOREN', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp."zieldatum" IS NULL 
             OR TRIM(opp."zieldatum") = '' 
             OR UPPER(TRIM(opp."zieldatum")) = 'N/A'
             OR TRIM(opp."zieldatum") = '0000-00-00' THEN NULL
        WHEN TRIM(opp."zieldatum") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(opp."zieldatum"), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(opp."zieldatum") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp."zieldatum"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(opp."zieldatum") ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(opp."zieldatum"), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(opp."zieldatum") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp."zieldatum"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
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
              '\.', '')::DOUBLE PRECISION
        END AS "Amount",
    CASE
        WHEN opp."waehrungscode" IS NULL OR TRIM(opp."waehrungscode") = '' THEN 'EUR'
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(opp."waehrungscode")) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM(opp."waehrungscode")) = 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    C.kundennummer AS "AccountId",
    opp."opp_kennung" AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_opportunities') }} opp
LEFT JOIN (
    SELECT 
        "kundennummer",
        REGEXP_REPLACE("kundennummer", '_DUP$', '') AS clean_key
    FROM {{ source('fixture_master_src', 'master_kunden') }}
) C ON TRIM(opp."kunden_ref") = C.clean_key

WHERE opp."opp_kennung" IS NOT NULL