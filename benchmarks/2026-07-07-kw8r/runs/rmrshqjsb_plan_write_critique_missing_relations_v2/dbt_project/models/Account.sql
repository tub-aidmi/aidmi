{{ config(materialized='table') }}

SELECT 
    "id" AS "Id",
    COALESCE(TRIM(INITCAP(NULLIF(TRIM("name"), ''))), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE WHEN UPPER(TRIM("tier")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(TRIM("tier")) ELSE NULL END AS "Customer_Tier__c",
    "region" AS "Region__c",
    "industry" AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    "id" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}