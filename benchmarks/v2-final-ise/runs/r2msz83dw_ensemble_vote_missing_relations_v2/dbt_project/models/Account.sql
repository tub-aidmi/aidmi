{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
    CASE 
        WHEN id ~ '^ACC-(\d+)$' THEN regexp_replace(id, '^ACC-', '')
        ELSE NULL 
    END AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(tier)) IN ('gold', 'silver', 'bronze', 'platinum') 
            THEN INITCAP(TRIM(tier))
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}