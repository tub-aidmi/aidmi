{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    CAST(NULL AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN account.tier IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN account.tier
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region AS "Region__c",
    account.industry AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
