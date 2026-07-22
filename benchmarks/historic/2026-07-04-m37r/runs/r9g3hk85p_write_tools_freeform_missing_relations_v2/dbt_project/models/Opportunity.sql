{{
  config(
    materialized='table'
  )
}}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    COALESCE(CASE
        WHEN stage = 'Prospecting' THEN 'Prospecting'
        WHEN stage = 'Qualification' THEN 'Qualification'
        WHEN stage = 'Closed Lost' THEN 'Closed Lost'
        WHEN stage = 'Closed Won' THEN 'Closed Won'
        ELSE NULL
    END, 'Prospecting') AS "StageName",
    NULL AS "CloseDate",
    amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    customer_number AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
