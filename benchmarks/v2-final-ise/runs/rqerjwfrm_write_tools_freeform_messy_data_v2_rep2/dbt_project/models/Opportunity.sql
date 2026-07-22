{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualify') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs_analysis', 'needs') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value_prop', 'value') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'id decision makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN amount ~ '^[\d]+[.,\d]+$' THEN 
            CASE 
                WHEN amount ~ '\.' AND amount ~ ',' THEN 
                    -- European format: 1.234,56 -> 1234.56
                    CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
                WHEN amount ~ ',' THEN 
                    -- US format with comma as thousand separator: 1,234.56
                    CAST(REPLACE(amount, ',', '') AS DOUBLE PRECISION)
                ELSE 
                    -- Plain number or dot as decimal
                    CAST(amount AS DOUBLE PRECISION)
            END
        WHEN amount ~ '^[\d]+$' THEN CAST(amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(currencyisocode), '') AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
