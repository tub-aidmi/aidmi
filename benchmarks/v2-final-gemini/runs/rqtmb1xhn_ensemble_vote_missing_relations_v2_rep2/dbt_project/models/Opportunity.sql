{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(opp.stage) LIKE '%qualif%' THEN 'Qualification'
        WHEN LOWER(opp.stage) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(opp.stage) LIKE '%value prop%' THEN 'Value Proposition'
        WHEN LOWER(opp.stage) LIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.stage) LIKE '%perception analysis%' THEN 'Perception Analysis'
        WHEN LOWER(opp.stage) LIKE '%proposal%' OR LOWER(opp.stage) LIKE '%quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.stage) LIKE '%negotiat%' OR LOWER(opp.stage) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(opp.stage) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(opp.stage) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NULL or unmapped stages, satisfying NOT NULL
    END AS "StageName",
    to_char(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- No source field, defaulting to current date as text
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Defaulting as no source field
    acc.id AS "AccountId", -- Joining to account source table
    opp.id AS "Legacy_Opportunity_ID__c",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    opp.customer_number = acc.id
