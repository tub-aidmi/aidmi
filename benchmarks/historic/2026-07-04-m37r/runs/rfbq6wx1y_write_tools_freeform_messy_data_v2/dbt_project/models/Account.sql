{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'Unnamed Account ' || TRIM(account.id)) AS "Name",
    TRIM(account.erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'platinum' THEN 'Platinum'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region__c) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    TRIM(account.website) AS "Website",
    TRIM(account.billingcity) AS "BillingCity",
    TRIM(account.billingcountry) AS "BillingCountry",
    TRIM(account.legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
WHERE
    account.id IS NOT NULL
