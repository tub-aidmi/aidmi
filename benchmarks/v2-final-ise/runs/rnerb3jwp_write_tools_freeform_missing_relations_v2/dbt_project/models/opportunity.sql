{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE 
        WHEN LOWER(o.stage) IN ('prospecting', 'lead', 'initial contact') THEN 'Prospecting'
        WHEN LOWER(o.stage) IN ('qualification', 'qualify', 'qualified') THEN 'Qualification'
        WHEN LOWER(o.stage) IN ('needs analysis', 'needs', 'requirement') THEN 'Needs Analysis'
        WHEN LOWER(o.stage) IN ('value proposition', 'value prop', 'proposal') THEN 'Value Proposition'
        WHEN LOWER(o.stage) IN ('identify decision makers', 'decision maker', 'idm', 'identifying decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) IN ('perception analysis', 'perception', 'competitive assessment') THEN 'Perception Analysis'
        WHEN LOWER(o.stage) IN ('proposal/price quote', 'proposal', 'price quote', 'quote', 'pricing') THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) IN ('negotiation/review', 'negotiation', 'review', 'negotiating') THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) IN ('closed won', 'won', 'closed_won') THEN 'Closed Won'
        WHEN LOWER(o.stage) IN ('closed lost', 'lost', 'closed_lost', 'cancelled') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
     'USD' AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON o.account_name = acc.name
