-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    opp.name AS "Name",
    CASE
        WHEN opp.stage = 'Prospecting' THEN 'Prospecting'
        WHEN opp.stage = 'Qualification' THEN 'Qualification'
        WHEN opp.stage = 'Closed Won' THEN 'Closed Won'
        WHEN opp.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for any unexpected stage, as StageName is NOT NULL
    END AS "StageName",
    '1900-01-01' AS "CloseDate", -- Default as source is missing and target is NOT NULL
    opp.amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default as source is missing
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Source is missing, target is nullable
    NULL AS "LastModifiedDate", -- Source is missing, target is nullable
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON opp.account_name = acc.name