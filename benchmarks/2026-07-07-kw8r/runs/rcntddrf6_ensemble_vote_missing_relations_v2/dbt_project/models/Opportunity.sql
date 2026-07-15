{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN o.stage = 'Value Proposition' THEN 'Value Proposition'
        WHEN o.stage = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN o.stage = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN o.stage = 'Proposal' THEN 'Proposal/Price Quote'
        WHEN o.stage = 'Negotiation' THEN 'Negotiation/Review'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    '1970-01-01' AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON o.customer_number = a.id