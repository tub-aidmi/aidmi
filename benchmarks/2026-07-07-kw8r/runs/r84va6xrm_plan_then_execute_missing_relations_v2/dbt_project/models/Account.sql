{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(REGEXP_REPLACE(id, '[^A-Z0-9]', '', 'g'))) AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE LOWER(TRIM(tier))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(LOWER(region)) AS "Region__c",
    TRIM(INITCAP(industry)) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}