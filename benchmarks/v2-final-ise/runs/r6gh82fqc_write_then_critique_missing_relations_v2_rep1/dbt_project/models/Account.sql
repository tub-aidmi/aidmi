{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown Account') AS "Name",
    NULL::TEXT AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    UPPER(TRIM(region)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}