{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account') AS "Name",
    account.id AS "ERP_Number__c",
    CASE
        WHEN INITCAP(TRIM(account.tier)) = 'Gold' THEN 'Gold'
        WHEN INITCAP(TRIM(account.tier)) = 'Silver' THEN 'Silver'
        WHEN INITCAP(TRIM(account.tier)) = 'Bronze' THEN 'Bronze'
        WHEN INITCAP(TRIM(account.tier)) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(INITCAP(account.region)) AS "Region__c",
    TRIM(INITCAP(account.industry)) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
