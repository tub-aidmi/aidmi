{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
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
        ELSE 'Prospecting'
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    REGEXP_REPLACE(o.customer_number, '^KD-', 'ACC-') = a.id
