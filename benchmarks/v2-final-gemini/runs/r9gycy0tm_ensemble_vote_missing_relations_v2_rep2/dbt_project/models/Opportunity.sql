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
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(p.go_live, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.account_name = a.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON o.id = p.opportunity_ref
