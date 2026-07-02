{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    COALESCE(TRIM("Name"), 'Unknown') AS "Name",
    CAST("ERP_Number__c" AS TEXT) AS "ERP_Number__c",

    CASE
        WHEN LOWER(TRIM(COALESCE("Customer_Tier__c", ''))) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(COALESCE("Customer_Tier__c", ''))) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(COALESCE("Customer_Tier__c", ''))) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(COALESCE("Customer_Tier__c", ''))) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",

    CASE
        WHEN TRIM(COALESCE("Region__c", '')) = '' THEN NULL
        ELSE INITCAP(TRIM("Region__c"))
    END AS "Region__c",

    INITCAP(TRIM("Industry")) AS "Industry",
    CAST(TRIM("Website") AS TEXT) AS "Website",
    INITCAP(TRIM("BillingCity")) AS "BillingCity",
    INITCAP(TRIM("BillingCountry")) AS "BillingCountry",
    CAST("Legacy_Customer_ID__c" AS TEXT) AS "Legacy_Customer_ID__c",

    '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Account') }}