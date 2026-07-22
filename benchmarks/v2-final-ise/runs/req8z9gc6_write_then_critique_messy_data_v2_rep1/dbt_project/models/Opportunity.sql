{{ config(materialized='table') }}
SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unnamed Opportunity') AS "Name",
    -- Map StageName to target enum values with NULL fallback for unmatched values
    CASE 
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting', '1 - prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stagename)) IN ('qualification', '2 - qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(o.stagename)) IN ('needs analysis', '3 - needs analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('value proposition', '4 - value proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stagename)) IN ('id. decision makers', '5 - id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stagename)) IN ('perception analysis', '6 - perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('proposal/price quote', '7 - proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stagename)) IN ('negotiation/review', '8 - negotiation/review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed won', '9 - closed won') THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed lost', '10 - closed lost') THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    -- Parse CloseDate to ISO format (YYYY-MM-DD)
    CASE 
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    -- Parse Amount to double precision
    CASE 
        WHEN o.amount ~ '^[0-9]+(\.[0-9]+)?$' THEN o.amount::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(o.amount, ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(o.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^\$[0-9]+(\.[0-9]+)?$' THEN REGEXP_REPLACE(o.amount, '^\$', '')::DOUBLE PRECISION
        WHEN o.amount ~ '^€[0-9]+([.,][0-9]+)?$' THEN REGEXP_REPLACE(REGEXP_REPLACE(o.amount, '^€', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL 
    END AS "Amount",
    -- CurrencyIsoCode: clean and uppercase
    COALESCE(NULLIF(TRIM(UPPER(o.currencyisocode)), ''), NULL) AS "CurrencyIsoCode",
    -- AccountId: map to Salesforce-style Account Id
    o.accountid AS "AccountId",
    -- Legacy_Opportunity_ID__c: populated from source natural key (id)
    o.id AS "Legacy_Opportunity_ID__c",
    -- CreatedDate and LastModifiedDate: default to NULL (not in source)
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o