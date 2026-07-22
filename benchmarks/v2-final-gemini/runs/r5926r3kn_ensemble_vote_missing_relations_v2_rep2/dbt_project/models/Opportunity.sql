{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.stage) = 'new' THEN 'Prospecting'
        WHEN LOWER(opp.stage) = 'discovery' THEN 'Qualification'
        WHEN LOWER(opp.stage) = 'solution design' THEN 'Needs Analysis'
        WHEN LOWER(opp.stage) = 'value proposal' THEN 'Value Proposition'
        WHEN LOWER(opp.stage) = 'decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.stage) = 'analysis' THEN 'Perception Analysis'
        WHEN LOWER(opp.stage) = 'quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.stage) = 'negotiation' THEN 'Negotiation/Review'
        WHEN LOWER(opp.stage) = 'won' THEN 'Closed Won'
        WHEN LOWER(opp.stage) = 'lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped or NULL stages, as it's NOT NULL
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON opp.customer_number = acc.id