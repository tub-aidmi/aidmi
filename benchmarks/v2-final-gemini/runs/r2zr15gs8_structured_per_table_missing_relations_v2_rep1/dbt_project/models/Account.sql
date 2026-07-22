{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(account.tier)) = 'GOLD' THEN 'Gold'
        WHEN TRIM(UPPER(account.tier)) = 'SILVER' THEN 'Silver'
        WHEN TRIM(UPPER(account.tier)) = 'BRONZE' THEN 'Bronze'
        WHEN TRIM(UPPER(account.tier)) = 'PLATINUM' THEN 'Platinum'
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
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
