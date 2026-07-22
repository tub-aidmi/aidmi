{{
    config(materialized='table')
}}

SELECT
    acc.id AS "Id",
    COALESCE(acc.name, 'Unknown Account') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(acc.tier) = 'gold' THEN 'Gold'
        WHEN LOWER(acc.tier) = 'silver' THEN 'Silver'
        WHEN LOWER(acc.tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(acc.tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    acc.region AS "Region__c",
    acc.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    acc.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc