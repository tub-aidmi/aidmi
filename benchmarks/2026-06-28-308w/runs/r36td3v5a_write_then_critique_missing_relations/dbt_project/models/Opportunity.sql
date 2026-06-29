
{{ config(materialized='table') }}

WITH accounts_deduped AS (
    SELECT
        LOWER(name) AS account_name_lower,
        MIN(id) AS account_id
    FROM {{ source('fixture_missing_relations_src', 'Account') }}
    WHERE name IS NOT NULL
    GROUP BY LOWER(name)
)
SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN opp.stage = 'Prospecting' THEN 'Prospecting'
        WHEN opp.stage = 'Qualification' THEN 'Qualification'
        WHEN opp.stage = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN opp.stage = 'Value Proposition' THEN 'Value Proposition'
        WHEN opp.stage = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN opp.stage = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN opp.stage = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN opp.stage = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN opp.stage = 'Closed Won' THEN 'Closed Won'
        WHEN opp.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    acc_deduped.account_id AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Opportunity') }} opp
LEFT JOIN
    accounts_deduped acc_deduped
    ON LOWER(opp.account_name) = acc_deduped.account_name_lower
