{{ config(materialized='table') }}
SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stagename)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(o.stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('value proposition', 'value_prop') THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stagename)) IN ('id. decision makers', 'identify decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stagename)) IN ('proposal/price quote', 'proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stagename)) IN ('negotiation/review', 'negotiation', 'review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stagename)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    CASE 
        WHEN o.amount ~ '^\d{1,3}(\.\d{3})*(,\d+)?$|^\d+(\.\d{2})?$|^\d+$' THEN 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(o.amount, '[^\d.,-]', '', 'g'),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) AS DOUBLE PRECISION
            )
        ELSE NULL 
    END AS "Amount",
    NULLIF(TRIM(o.currencyisocode), '') AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON o.accountid = a.id