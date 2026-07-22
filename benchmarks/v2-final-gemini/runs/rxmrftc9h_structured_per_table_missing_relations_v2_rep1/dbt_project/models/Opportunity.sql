{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN o.stage ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN o.stage ILIKE 'Qualification' THEN 'Qualification'
        WHEN o.stage ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN o.stage ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN o.stage ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN o.stage ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN o.stage ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN o.stage ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN o.stage ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN o.stage ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON 'ACC-' || SUBSTRING(o.customer_number FROM 4) = a.id
