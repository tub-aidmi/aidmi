{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE
        WHEN LOWER(account.tier) = 'gold' THEN 'Gold'
        WHEN LOWER(account.tier) = 'silver' THEN 'Silver'
        WHEN LOWER(account.tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(account.tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region AS "Region__c",
    account.industry AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
