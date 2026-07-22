{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(src.tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(src.tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(src.tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(src.tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(src.region) AS "Region__c",
    TRIM(src.industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    src.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src