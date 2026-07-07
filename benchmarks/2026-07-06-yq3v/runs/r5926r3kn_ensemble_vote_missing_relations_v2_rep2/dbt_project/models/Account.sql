{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(TRIM(account.name), 'Unknown') AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(account.tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(account.tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(account.tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(account.tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
