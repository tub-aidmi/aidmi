{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(a.tier)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM(a.tier)))
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(a.region) AS "Region__c",
    TRIM(a.industry) AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    a.id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a