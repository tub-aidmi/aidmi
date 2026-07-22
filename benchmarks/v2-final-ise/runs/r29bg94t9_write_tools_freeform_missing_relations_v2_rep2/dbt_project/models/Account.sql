{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER(tier)) = 'platinum' THEN 'Platinum'
        WHEN TRIM(LOWER(tier)) = 'gold' THEN 'Gold'
        WHEN TRIM(LOWER(tier)) = 'silver' THEN 'Silver'
        WHEN TRIM(LOWER(tier)) = 'bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(region), '') AS "Region__c",
    NULLIF(TRIM(industry), '') AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
