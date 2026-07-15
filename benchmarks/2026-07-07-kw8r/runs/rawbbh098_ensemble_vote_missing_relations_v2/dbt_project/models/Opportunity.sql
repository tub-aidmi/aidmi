{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT 
        opp.id,
        opp.name,
        opp.stage,
        opp.amount,
        opp.customer_number,
        opp.account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
),
account_mapping AS (
    SELECT 
        acc.id AS account_id,
        acc.name AS account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
)

SELECT 
    opp.id AS "Id",
    opp.name AS "Name",
    opp.stage AS "StageName",
    NULL::text AS "CloseDate",
    opp.amount AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    acc.account_id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_source AS opp
LEFT JOIN account_mapping AS acc ON TRIM(opp.account_name) = TRIM(acc.account_name)