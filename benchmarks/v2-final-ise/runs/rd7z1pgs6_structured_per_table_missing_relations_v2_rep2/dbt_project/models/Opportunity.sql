{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    o.stage AS "StageName",
    NULL::text AS "CloseDate",
    o.amount AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    REPLACE(o.customer_number, 'KD-', 'ACC-') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o