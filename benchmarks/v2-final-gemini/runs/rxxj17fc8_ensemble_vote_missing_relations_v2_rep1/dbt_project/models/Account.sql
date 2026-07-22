{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unknown Account') AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE
        WHEN LOWER(src.tier) = 'gold' THEN 'Gold'
        WHEN LOWER(src.tier) = 'silver' THEN 'Silver'
        WHEN LOWER(src.tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(src.tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    src.region AS "Region__c",
    src.industry AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    src.id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src
