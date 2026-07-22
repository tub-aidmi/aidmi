{{ config(materialized='table') }}

WITH normalised_customer_keys AS (
    SELECT 
        TRIM(kundennummer) AS raw_key,
        REGEXP_REPLACE(UPPER(TRIM(kundennummer)), '^(KUN-|KUNDEN-|CUST-)', '') AS clean_key
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
cleaned_opportunities AS (
    SELECT 
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        -- Clean customer reference using same logic as Account hub
        UPPER(TRIM(REGEXP_REPLACE(kunden_ref, '^(KUN-|KUNDEN-|CUST-)', ''))) AS clean_customer_key
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
parsed_amounts AS (
    SELECT 
        *,
        -- Parse amount: strip currency symbols/text, handle European format
        CASE 
            WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
            ELSE
                CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(TRIM(REGEXP_REPLACE(auftragswert, 'EUR|€|CHF|GBP|USD|\$', ''))),
                            '[^0-9.,]', ''),  -- Remove all non-numeric except dots and commas
                        '\.(\d{3})(?=\D|$)', ''),  -- Remove thousand separator dots
                    ',')  -- Replace comma with period for decimal as string, then cast
                AS DOUBLE PRECISION)
        END AS parsed_amount
    FROM cleaned_opportunities
),
parsed_dates AS (
    SELECT 
        *,
        -- Parse CloseDate supporting multiple formats
        CASE 
            WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN zieldatum ~ '^\d{4}\d{2}\d{2}$' THEN  -- YYYYMMDD format
                TO_DATE(zieldatum, 'YYYYMMDD')::TEXT
            WHEN zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN  -- DD.MM.YYYY format
                TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
            WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN  -- MM/DD/YYYY format
                TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS parsed_close_date
    FROM parsed_amounts
)
SELECT 
    TRIM(opp_kennung) AS "Id",
    COALESCE(INITCAP(TRIM(titel)), 'Unnamed Opportunity') AS "Name",
    -- Map German sales pipeline stages to standard target enum
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
        ELSE NULL  -- Fallback for unmapped stages
    END AS "StageName",
    parsed_close_date AS "CloseDate",
    parsed_amount AS "Amount",
    COALESCE(UPPER(TRIM(waehrungscode)), 'EUR') AS "CurrencyIsoCode",
    ck.clean_key AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_dates pd
LEFT JOIN normalised_customer_keys ck 
    ON TRIM(LOWER(pd.clean_customer_key)) = TRIM(LOWER(ck.raw_key));