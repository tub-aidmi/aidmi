{{ config(materialized='table') }}

WITH cleaned_amounts AS (
    SELECT 
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        -- Strip currency text prefixes from amount field
        REGEXP_REPLACE(
            TRIM(COALESCE(NULLIF(amount, ''), '')), 
            '^(EUR|USD|GBP|CHF|Dollar|Euro|£|€)\s*', '', 
            'i'
        ) AS raw_amount
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    -- Id: use id directly, trim and uppercase to match Salesforce format if needed
    TRIM(COALESCE(id, '')) AS "Id",
    
    -- Name: initialize empty strings with a default
    INITCAP(TRIM(COALESCE(NULLIF(name, ''), 'Unknown'))) AS "Name",
    
    -- StageName: map all variants to standardized enum values
    CASE UPPER(TRIM(COALESCE(NULLIF(stagename, ''), '')))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'PROSPECT'    THEN 'Prospecting'
        WHEN 'IN KONTAKT'  THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'QUALI'       THEN 'Qualification'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'IN PRÜFUNG' THEN 'Qualification'
        WHEN 'CLOSED WON'   THEN 'Closed Won'
        WHEN 'WON'          THEN 'Closed Won'
        WHEN 'GEWONNEN'     THEN 'Closed Won'
        WHEN 'ABSCHLUSS (GEWONNEN)' THEN 'Closed Won'
        WHEN 'ABSCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
        WHEN 'CLOSED LOST'  THEN 'Closed Lost'
        WHEN 'LOST'         THEN 'Closed Lost'
        WHEN 'VERLOREN'     THEN 'Closed Lost'
        WHEN 'ABSCHLUSS (VERLOREN)' THEN 'Closed Lost'
        WHEN 'ABSCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    
    -- CloseDate: parse multiple date formats to ISO YYYY-MM-DD
    CASE
        WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN
            TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    
    -- Amount: handle multiple formats with currency prefix stripping
    CASE 
        WHEN raw_amount IS NULL OR raw_amount = '' THEN NULL
        WHEN UPPER(raw_amount) IN ('NONE', 'NULL', 'N/A', '-1', '0') THEN 0.0
        ELSE
            CASE
                -- European format with dot-thousand-separator and comma decimal: "60.702,05" -> 60702.05
                WHEN raw_amount ~ '^\s*[-+]?\d{1,3}(\.\d{3})+,\d{1,2}\s*$' THEN
                    CAST(REGEXP_REPLACE(REGEXP_REPLACE(raw_amount, '[\s]+', ''), '\\.', ''), ',')::DOUBLE PRECISION
                
                -- Standard decimal: "42543.61", "-383632.13" 
                WHEN raw_amount ~ '^\s*[-+]?\d+(\.\d+)?$' THEN
                    CAST(REGEXP_REPLACE(raw_amount, '[\s]+', '') AS DOUBLE PRECISION)
                
                -- European without thousand-sep: "1234,56" -> 1234.56
                WHEN raw_amount ~ '^\s*[-+]?\d+,\d{1,2}\s*$' THEN
                    CAST(REGEXP_REPLACE(REGEXP_REPLACE(raw_amount, '[\s]+', ''), ',', '.') AS DOUBLE PRECISION)
                
                -- Malformed or unrecognized - try removing all non-numeric chars except . and - 
                WHEN raw_amount ~ '[-]?\d+(\.\d+)?[,]\d+' THEN
                    CAST(REGEXP_REPLACE(REGEXP_REPLACE(raw_amount, '[\s]+', ''), ',', '.') AS DOUBLE PRECISION)
                
                -- Pure integer or malformed with multiple dots (e.g., "60.702.05" is bad data - return 0)
                ELSE 0.0
            END
    END AS "Amount",

    -- CurrencyIsoCode: normalize to uppercase standard codes
    CASE LOWER(TRIM(COALESCE(NULLIF(currencyisocode, ''), '')))
        WHEN 'eur' THEN 'EUR'
        WHEN 'euro' THEN 'EUR'
        WHEN 'usd' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN 'chf' THEN 'CHF'
        WHEN 'gbp' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        WHEN '€' THEN 'EUR'
        ELSE NULL
    END AS "CurrencyIsoCode",

    -- AccountId: reference the Salesforce-style Account Id from opportunity.accountid
    TRIM(COALESCE(accountid, '')) AS "AccountId",

    -- Legacy_Opportunity_ID__c: store the source natural key
    TRIM(COALESCE(id, '')) AS "Legacy_Opportunity_ID__c",

    -- Timestamps and flag columns
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM cleaned_amounts