{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Account Name') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN UPPER(src.tier) = 'GOLD' THEN 'Gold'
        WHEN UPPER(src.tier) = 'SILVER' THEN 'Silver'
        WHEN UPPER(src.tier) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(src.tier) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    src.region AS "Region__c",
    src.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    src.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src
