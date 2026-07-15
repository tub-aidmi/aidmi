{{ config(materialized='table') }}

SELECT
    CAST(o.id AS text) AS "Id",
    COALESCE(TRIM(o.name), 'Opportunity - ' || o.id) AS "Name",
    COALESCE(
        CASE 
            WHEN LOWER(TRIM(o.stage)) IN ('prospecting', 'new lead', 'lead') THEN 'Prospecting'
            WHEN LOWER(TRIM(o.stage)) IN ('qualification', 'qualify', 'qualified') THEN 'Qualification'
            WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'need analysis', 'discovery') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'value prop', 'solution', 'proposition') THEN 'Value Proposition'
            WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identify decision makers', 'finding decision makers') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(o.stage)) IN ('perception analysis', 'analysis', 'evaluation', 'assessment') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(o.stage)) IN ('proposal/price quote', 'proposal', 'quoting', 'quote') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(o.stage)) IN ('negotiation/review', 'negotiate', 'review') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won', 'accepted') THEN 'Closed Won'
            WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost', 'rejected') THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    CAST(a.id AS text) AS "AccountId",
    COALESCE(CAST(o.customer_number AS text), o.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(o.account_name) = TRIM(a.name)