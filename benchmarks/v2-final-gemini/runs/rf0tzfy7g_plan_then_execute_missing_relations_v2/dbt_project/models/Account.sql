{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    CAST(NULL AS TEXT) AS "ERP_Number__c",
    CASE UPPER(account.tier)
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
