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
        WHEN o.stage = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN o.stage = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL::text AS "CloseDate",
    o.amount AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    COALESCE(
        a.id,
        CASE WHEN o.customer_number LIKE 'KD-%' THEN REPLACE(o.customer_number, 'KD-', 'ACC-') ELSE NULL END
    ) AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON TRIM(o.account_name) = TRIM(a.name)