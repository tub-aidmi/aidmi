{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE LOWER(TRIM(account.customer_tier__c))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'silber' THEN 'Silver' -- Handle German "SILBER"
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum' -- Handle "Platin"
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    account.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- No direct source, defaulting to NULL as text
    NULL AS "LastModifiedDate", -- No direct source, defaulting to NULL as text
    0 AS "IsDeleted" -- No direct source, defaulting to 0 (false)
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
