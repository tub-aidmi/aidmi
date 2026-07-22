-- depends_on: 
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN o.stage ILIKE 'Prospect%' THEN 'Prospecting'
        WHEN o.stage ILIKE 'Qual%' THEN 'Qualification'
        WHEN o.stage ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN o.stage ILIKE 'Value Prop%' THEN 'Value Proposition'
        WHEN o.stage ILIKE 'Decision Maker%' THEN 'Id. Decision Makers'
        WHEN o.stage ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN o.stage ILIKE 'Proposal%' OR o.stage ILIKE 'Quote%' THEN 'Proposal/Price Quote'
        WHEN o.stage ILIKE 'Negotiation%' THEN 'Negotiation/Review'
        WHEN o.stage ILIKE 'Won' THEN 'Closed Won'
        WHEN o.stage ILIKE 'Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    '1900-01-01' AS "CloseDate",
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
    o.customer_number = a.id