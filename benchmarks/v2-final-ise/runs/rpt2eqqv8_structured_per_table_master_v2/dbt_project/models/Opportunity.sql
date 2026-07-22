{{ config(materialized='table') }}

SELECT 
    -- Transform to Salesforce-style Opportunity Id (consistent cross-table key format)
    '006' || LEFT(TRIM(opp_kennung), 15) AS "Id",

    -- Opportunity name from title
    INITCAP(TRIM(titel)) AS "Name",

    -- Map German sales phases to Salesforce Opportunity Stages
    CASE LOWER(TRIM(vertriebsphase))
        WHEN 'anfrage' THEN 'Prospecting'
        WHEN 'qualifizierung' THEN 'Qualification'
        WHEN 'bedarfsermittlung' THEN 'Needs Analysis'
        WHEN 'wertproposition' THEN 'Value Proposition'
        WHEN 'entscheidungsfindung' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'angebot/preise' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/bewertung' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE 'Prospecting'   -- fallback default
    END AS "StageName",

    -- Parse CloseDate: handle DD.MM.YYYY, YYYY-MM-DD, YYYYMMDD formats; output ISO text
    -- Fallback sentinel for missing/unparseable dates to satisfy NOT NULL constraint
    CASE 
        WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN '9999-12-31'
        WHEN LENGTH(TRIM(zieldatum)) = 8 AND TRIM(zieldatum) ~ '^\d{8}$' THEN 
            TO_DATE(TRIM(zieldatum), 'YYYYMMDD')::TEXT
        WHEN LENGTH(TRIM(zieldatum)) = 10 AND TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN 
            TO_DATE(TRIM(zieldatum), 'YYYY-MM-DD')::TEXT
        WHEN LENGTH(TRIM(zieldatum)) = 10 AND TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
        ELSE '9999-12-31'
    END AS "CloseDate",

    -- Parse Amount: handle European format, plain numeric, and text-prefixed values
    CASE 
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
         -- European locale: dots as thousand separators, comma as decimal point (e.g. 1.234,56)
        WHEN TRIM(auftragswert) ~ '^\s*[-+]?\d{1,3}(\.\d{3})+\,\d+\s*$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '\.', ''), ',', '.') AS DOUBLE PRECISION)
         -- Plain numeric with optional decimal (e.g. 14489369 or 1234.56)
        WHEN TRIM(auftragswert) ~ '^\s*[-+]?\d+(\.\d+)?\s*$' THEN 
            CAST(TRIM(auftragswert) AS DOUBLE PRECISION)
         -- Contains alphabetic characters, symbols, or other non-numeric prefixes/symbols
         -- Strip them out; only cast if digits remain after cleanup
        ELSE 
            CASE 
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(auftragswert), '\.', ''), ',', ''), '[^0-9.-]+', '') ~ '^\s*[-+]?\d+(\.\d+)?\s*$' THEN 
                    CAST(REGEXP_REPLACE(REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(auftragswert), '\.', ''), ',', ''), '[^0-9.-]+', '') AS DOUBLE PRECISION)
                ELSE NULL
            END
    END AS "Amount",

    -- Currency code (standard 3-letter ISO), uppercase
    UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",

    -- AccountId: transform kunden_ref to match the Salesforce Account Id format used by the Account model ('001' prefix)
    '001' || LEFT(TRIM(kunden_ref), 15) AS "AccountId",

    -- Legacy Opportunity natural key for row-level verification
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",

    -- Audit columns not present in source system; provide defaults since target is NOT NULL
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}

WHERE opp_kennung IS NOT NULL 
  AND TRIM(opp_kennung) != ''