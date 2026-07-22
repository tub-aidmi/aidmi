{{
    config(materialized='table')
}}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN o.stage ILIKE 'New Opportunity' THEN 'Prospecting'
        WHEN o.stage ILIKE 'Initial Contact' THEN 'Prospecting'
        WHEN o.stage ILIKE 'Discovery' THEN 'Qualification'
        WHEN o.stage ILIKE 'Qualify' THEN 'Qualification'
        WHEN o.stage ILIKE 'Needs Identified' THEN 'Needs Analysis'
        WHEN o.stage ILIKE 'Value Prop' THEN 'Value Proposition'
        WHEN o.stage ILIKE 'Decision Makers Identified' THEN 'Id. Decision Makers'
        WHEN o.stage ILIKE 'Perception Check' THEN 'Perception Analysis'
        WHEN o.stage ILIKE 'Proposal Sent' THEN 'Proposal/Price Quote'
        WHEN o.stage ILIKE 'Negotiation' THEN 'Negotiation/Review'
        WHEN o.stage ILIKE 'Review' THEN 'Negotiation/Review'
        WHEN o.stage ILIKE 'Won' THEN 'Closed Won'
        WHEN o.stage ILIKE 'Closed Success' THEN 'Closed Won'
        WHEN o.stage ILIKE 'Lost' THEN 'Closed Lost'
        WHEN o.stage ILIKE 'Closed Failed' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unknown stages, as StageName is NOT NULL
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
    o.customer_number = a.id