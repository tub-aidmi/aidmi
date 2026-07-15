{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    INITCAP(TRIM(COALESCE(name, ''))) AS "Name",
    
     -- StageName: Map source values to target enum domain with NULL fallback for unrecognized values
    CASE UPPER(TRIM(COALESCE(stagename, '')))
        WHEN 'NEW' THEN 'Prospecting'
        WHEN 'OPEN' THEN 'Qualification'
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

     -- CloseDate: Parse multiple formats, output ISO YYYY-MM-DD
    CASE 
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",

     -- Amount: Clean text, handle European/US formats, guard empty strings before cast
    CASE 
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
         -- European format: digits.dot3digits comma digits   (e.g. '1.234,56')
        WHEN amount ~ '\.\d{3},\d+$' THEN 
            CAST((REPLACE(REPLACE(TRIM(amount), '.', ''), ',', '.')::DOUBLE PRECISION) AS DOUBLE PRECISION)
         -- US format: digits comma3digits dot digits   (e.g. '1,234.56')
        WHEN amount ~ ',\d{3}\.\d+$' THEN 
            CAST((REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,\-]', '', 'g'), ',', '')::DOUBLE PRECISION) AS DOUBLE PRECISION)
         -- Simple number or cents: strip non-numeric (keep digits, dot, minus), guard empty result
        ELSE CASE 
                 WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.\-]', '', 'g') ~ '^\d+(\.\d+)?$' 
                 THEN CAST((REGEXP_REPLACE(TRIM(amount), '[^0-9.\-]', '', 'g'))::DOUBLE PRECISION AS DOUBLE PRECISION)
                 ELSE NULL
             END
    END AS "Amount",

    UPPER(TRIM(COALESCE(currencyisocode, ''))) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}