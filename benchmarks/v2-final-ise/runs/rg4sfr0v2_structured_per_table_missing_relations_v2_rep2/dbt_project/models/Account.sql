{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    a.id AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(a.tier)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') 
        THEN INITCAP(LOWER(TRIM(a.tier)))
        ELSE NULL
    END AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    a.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a