{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    COALESCE(o.stage, 'Prospecting') AS "StageName",
    '1900-01-01' AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    REPLACE(o.customer_number, 'KD-', 'ACC-') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
;