-- depends_on: {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
-- depends_on: {{ source('fixture_missing_relations_v2_src', 'account') }}

{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(opp.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.stage)) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%negotiation%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%qualification%' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%value proposition%' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.stage)) LIKE '%perception analysis%' THEN 'Perception Analysis'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR('2099-12-31'::DATE, 'YYYY-MM-DD') AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
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
    LOWER(TRIM(opp.account_name)) = LOWER(TRIM(acc.name))