{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stage)) IN ('prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) IN ('qualify', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'requirement', 'requirements') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) IN ('identify decision makers', 'decision maker', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) IN ('perception analysis', 'perception') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL 
    END AS "StageName",
    CAST(NULL AS TEXT) AS "CloseDate",
    o.amount AS "Amount",
    CAST('USD' AS TEXT) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON LOWER(TRIM(o.account_name)) = LOWER(TRIM(a.name))