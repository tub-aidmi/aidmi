{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(INITCAP(TRIM(account.name)), 'Unknown') AS "Name",
    TRIM(account.erp_number__c) AS "ERP_Number__c",
    CASE LOWER(TRIM(account.customer_tier__c))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(account.region__c)) AS "Region__c",
    INITCAP(TRIM(account.industry)) AS "Industry",
    TRIM(account.website) AS "Website",
    INITCAP(TRIM(account.billingcity)) AS "BillingCity",
    INITCAP(TRIM(account.billingcountry)) AS "BillingCountry",
    TRIM(account.legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
