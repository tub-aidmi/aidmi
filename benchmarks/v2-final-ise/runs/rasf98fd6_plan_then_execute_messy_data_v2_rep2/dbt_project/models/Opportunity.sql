{{ config(materialized='table') }}

WITH opportunity_raw AS (
    SELECT *
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
account_map AS (
    -- Normalize account ids from both sources to ensure join alignment
    SELECT 
        TRIM(id) AS raw_id,
        INITCAP(TRIM(REGEXP_REPLACE(id, '^(CUST-|A-)', '', 'i'))) AS canonical_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),
normalized_opportunity AS (
    SELECT
        -- Id: normalize to INITCAP, strip leading zeros from numeric part
        UPPER(TRIM(LEFT(id, 4))) || '-' || LPAD(RIGHT(id, LENGTH(id) - 4)::INTEGER, 5, '0') AS "Id",
        
        -- Name: INITCAP and trim
        INITCAP(TRIM(name)) AS "Name",
        
        -- StageName: map to target enum domain
        CASE 
            WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
            WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
            WHEN UPPER(TRIM(stagename)) IN ('NEEDS ANALYSIS', 'IN PRÜFUNG') THEN 'Needs Analysis'
            WHEN UPPER(TRIM(stagename)) IN ('VALUE PROPOSITION', 'WERTEPROPOSITION') THEN 'Value Proposition'
            WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS', 'ID. ENTSCHEIDER') THEN 'Id. Decision Makers'
            WHEN UPPER(TRIM(stagename)) IN ('PERCEPTION ANALYSIS', 'WIRKUNGSANALYSE') THEN 'Perception Analysis'
            WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGABE') THEN 'Proposal/Price Quote'
            WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/ÜBERPRÜFUNG') THEN 'Negotiation/Review'
            WHEN UPPER(TRIM(stagename)) IN ('WON', 'CLOSED WON', 'ABSCHLIESSANG (GEWONNEN)', 'GESCHLOSSEN GEWONNEN', 'GESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
            WHEN UPPER(TRIM(stagename)) IN ('LOST', 'CLOSED LOST', 'VERLOREN', 'ABSCHLIESANG (VERLOREN)', 'GESCHLOSSEN VERLOREN', 'GESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        
        -- CloseDate: robust multi-format parser to ISO YYYY-MM-DD
        CASE 
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
            WHEN closedate ~ '^\d{8}$' THEN 
                SUBSTR(closedate, 1, 4) || '-' || SUBSTR(closedate, 5, 2) || '-' || SUBSTR(closedate, 7, 2)
            WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
                TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
                TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "CloseDate",
        
        -- Amount: strip currency symbols, handle European format (dot=thousands, comma=decimal)
        CASE 
            WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
            ELSE
                CASE 
                    -- Pattern with dot and comma suggesting European format (e.g., 60.702,05 or 377.160,56)
                    WHEN amount ~ '^\-?[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
                        (REGEXP_REPLACE(REGEXP_REPLACE(amount, '[€$£\$]', '', 'g'), '^(\d+)\.(\d{3},\d+)$', '\1\2', 'i')::TEXT)::DOUBLE PRECISION
                    -- Pattern with comma as decimal (e.g., 60702,05)
                    WHEN amount ~ '^\-?[0-9]+,[0-9]{2}$' THEN 
                        REGEXP_REPLACE(amount, '[€$£\$]', '', 'g')::DOUBLE PRECISION
                    -- Pattern with dot as decimal (e.g., 42092.26)
                    WHEN amount ~ '^\-?[0-9]+\.[0-9]{1,2}$' THEN 
                        REGEXP_REPLACE(amount, '[€$£\$]', '', 'g')::DOUBLE PRECISION
                    -- Pattern with currency prefix and dot decimal (e.g., EUR 159619.38)
                    WHEN amount ~ '^([A-Z]+ )\-?[0-9]+\.[0-9]{2}$' THEN 
                        SUBSTR(REGEXP_REPLACE(amount, '^[A-Z]+ ', ''), POSITION(' ' IN REGEXP_REPLACE(amount, '^[A-Z]+ ', '')) + 1)::DOUBLE PRECISION
                    ELSE NULL
                END
        END AS "Amount",
        
        -- CurrencyIsoCode: normalize to standard 3-letter codes
        CASE UPPER(TRIM(currencyisocode))
            WHEN 'USD' THEN 'USD'
            WHEN 'EUR' THEN 'EUR'
            WHEN 'GBP' THEN 'GBP'
            WHEN 'CHF' THEN 'CHF'
            WHEN 'EURO' THEN 'EUR'
            WHEN 'DOLLAR' THEN 'USD'
            WHEN '$' THEN 'USD'
            WHEN '£' THEN 'GBP'
            WHEN '€' THEN 'EUR'
            ELSE UPPER(TRIM(currencyisocode))
        END AS "CurrencyIsoCode",
        
        -- AccountId: normalize to match Account.Id format (INITCAP, strip CUST- prefix for canonical matching)
        INITCAP(TRIM(REGEXP_REPLACE(a.accountid_normalized, '^(CUST|A)-', '', 'i'))) AS "AccountId",
        
        -- Legacy_Opportunity_ID__c: direct copy of source id
        TRIM(id) AS "Legacy_Opportunity_ID__c",
        
        -- Audit fields with deterministic defaults
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
        
    FROM opportunity_raw o
    LEFT JOIN account_map a ON INITCAP(TRIM(REGEXP_REPLACE(o.accountid, '^(CUST|A)-', '', 'i'))) = a.canonical_id
)

SELECT * FROM normalized_opportunity