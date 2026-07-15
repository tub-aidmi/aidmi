{{ config(materialized='table') }}

WITH opp_raw AS (
    SELECT 
        TRIM(opp_kennung) AS raw_opp_id,
        INITCAP(TRIM(titel)) AS name,
        vertriebsphase,
        zieldatum,
        auftragswert,
        UPPER(TRIM(waehrungscode)) AS waehrungscode_raw,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

opp_cleaned AS (
    SELECT 
        raw_opp_id AS "Legacy_Opportunity_ID__c",
        COALESCE(NULLIF(name, ''), 'Unnamed Opportunity') AS "Name",
        CASE LOWER(TRIM(vertriebsphase))
            WHEN 'prospekting' THEN 'Prospecting'
            WHEN 'qualifikation' THEN 'Qualification'
            WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
            WHEN 'wertproposition' THEN 'Value Proposition'
            WHEN 'entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
            WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
            WHEN 'angebot/preisangebot' THEN 'Proposal/Price Quote'
            WHEN 'verhandlung/prüfung' THEN 'Negotiation/Review'
            WHEN 'abgeschlossen gewonnen' THEN 'Closed Won'
            WHEN 'abgeschlossen verloren' THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        CASE 
            WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN TO_DATE(TRIM(zieldatum), 'YYYYMMDD')::TEXT
            WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
            WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "CloseDate",
        CASE 
            WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
            WHEN TRIM(auftragswert) ~ '^\-?\d+(\.\d{1,2})?$' THEN 
                CAST(TRIM(auftragswert) AS DOUBLE PRECISION)
            WHEN TRIM(auftragswert) ~ '^\-?\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?$' THEN
                CAST(
                    REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.') AS DOUBLE PRECISION)
            ELSE NULL
        END AS "Amount",
        CASE 
            WHEN TRIM(waehrungscode_raw) IS NULL OR TRIM(waehrungscode_raw) = '' THEN 'EUR'
            ELSE waehrungscode_raw
        END AS "CurrencyIsoCode",
        -- Normalize customer reference: strip common prefixes and uppercase
        UPPER(TRIM(REGEXP_REPLACE(kunden_ref, '^(KUN-|KUNDEN-|CUST-)', ''))) AS clean_customer_key
    FROM opp_raw
),

account_lookup AS (
    SELECT 
        UPPER(TRIM(REGEXP_REPLACE(kundennummer, '^(KUN-|KUNDEN-|CUST-)', ''))) AS clean_kundennummer,
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT 
    oc."Legacy_Opportunity_ID__c" AS "Id",
    oc."Name",
    COALESCE(oc."StageName", 'Prospecting') AS "StageName",
    COALESCE(oc."CloseDate", '') AS "CloseDate",
    oc."Amount",
    oc."CurrencyIsoCode",
    al.clean_kundennummer AS "AccountId",
    oc."Legacy_Opportunity_ID__c" AS "Legacy_Opportunity_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opp_cleaned oc
LEFT JOIN account_lookup al 
    ON al.clean_kundennummer = oc.clean_customer_key