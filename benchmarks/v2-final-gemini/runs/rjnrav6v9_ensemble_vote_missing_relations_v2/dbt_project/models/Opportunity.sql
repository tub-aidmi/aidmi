{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN opp.stage ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN opp.stage ILIKE 'Qualification' THEN 'Qualification'
        WHEN opp.stage ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN opp.stage ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN opp.stage ILIKE 'Id. Decision Makers' OR opp.stage ILIKE 'Identify Decision Makers' THEN 'Id. Decision Makers'
        WHEN opp.stage ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN opp.stage ILIKE 'Proposal/Price Quote' OR opp.stage ILIKE 'Proposal' THEN 'Proposal/Price Quote'
        WHEN opp.stage ILIKE 'Negotiation/Review' OR opp.stage ILIKE 'Negotiation' THEN 'Negotiation/Review'
        WHEN opp.stage ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN opp.stage ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    opp.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    opp.customer_number = acc.id
