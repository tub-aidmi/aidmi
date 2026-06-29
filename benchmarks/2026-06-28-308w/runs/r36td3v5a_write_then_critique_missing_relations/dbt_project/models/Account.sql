
{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    CAST(NULL AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN account.tier = 'Gold' THEN 'Gold'
        WHEN account.tier = 'Silver' THEN 'Silver'
        WHEN account.tier = 'Bronze' THEN 'Bronze'
        WHEN account.tier = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region AS "Region__c",
    account.industry AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    CAST(NULL AS TEXT) AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Account') }} AS account
