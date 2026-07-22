{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account Name') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(account.tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(account.tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(account.tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(account.tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    TRIM(account.id) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }}
