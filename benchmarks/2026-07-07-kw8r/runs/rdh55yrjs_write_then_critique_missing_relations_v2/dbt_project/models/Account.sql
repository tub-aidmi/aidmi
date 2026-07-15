{{ config(materialized='table') }}

SELECT
    a."id" AS "Id",
    COALESCE(NULLIF(TRIM(a."name"), ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(a."tier")) IN ('gold', 'g') THEN 'Gold'
        WHEN LOWER(TRIM(a."tier")) IN ('silver', 's') THEN 'Silver'
        WHEN LOWER(TRIM(a."tier")) IN ('bronze', 'b') THEN 'Bronze'
        WHEN LOWER(TRIM(a."tier")) IN ('platinum', 'p') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    a."region" AS "Region__c",
    a."industry" AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    a."id" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a