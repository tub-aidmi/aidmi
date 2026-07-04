{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(account.tier) = 'gold' THEN 'Gold'
        WHEN LOWER(account.tier) = 'silver' THEN 'Silver'
        WHEN LOWER(account.tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(account.tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region AS "Region__c",
    account.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} AS account