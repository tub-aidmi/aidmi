{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(id)) AS "Id",
    INITCAP(TRIM(COALESCE(name, ''))) AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(INITCAP(region)) AS "Region__c",
    TRIM(INITCAP(industry)) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}