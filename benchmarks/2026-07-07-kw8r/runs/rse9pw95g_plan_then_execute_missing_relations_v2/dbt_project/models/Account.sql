{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    INITCAP(TRIM(COALESCE(name, ''))) AS "Name",
    CAST(NULL AS TEXT) AS "ERP_Number__c",
    CASE 
        WHEN INITCAP(LOWER(tier)) = 'gold' THEN 'Gold'
        WHEN INITCAP(LOWER(tier)) = 'silver' THEN 'Silver'
        WHEN INITCAP(LOWER(tier)) = 'bronze' THEN 'Bronze'
        WHEN INITCAP(LOWER(tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}