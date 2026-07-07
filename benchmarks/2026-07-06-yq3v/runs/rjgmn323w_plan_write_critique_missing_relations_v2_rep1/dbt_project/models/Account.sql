{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE LOWER(TRIM(account.tier))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
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
