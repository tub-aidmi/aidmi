{{ config(materialized='table') }}

SELECT
    CAST(TRIM(o.id) AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(o.name, 'Unnamed Opportunity'))) AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(TRIM(o.stage)) IN ('prospecting', 'new') THEN 'Prospecting'
            WHEN LOWER(TRIM(o.stage)) IN ('qualification', 'qualify') THEN 'Qualification'
            WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'discovery') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'proof of concept') THEN 'Value Proposition'
            WHEN LOWER(TRIM(o.stage)) IN ('id decision makers', 'identify decision maker') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(o.stage)) IN ('perception analysis', 'mental map') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'quote', 'proposal') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
            WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(REGEXP_REPLACE(o.customer_number, '^[^-]+-', '')) = TRIM(REGEXP_REPLACE(a.id, '^[^-]+-', ''))