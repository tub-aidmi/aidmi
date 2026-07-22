{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name", -- Name is NOT NULL, provide default
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(tier)) = 'bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    NULL AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Account') }}
