{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account Name') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN UPPER(account.tier) = 'GOLD' THEN 'Gold'
        WHEN UPPER(account.tier) = 'SILVER' THEN 'Silver'
        WHEN UPPER(account.tier) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(account.tier) = 'PLATINUM' THEN 'Platinum'
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
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
