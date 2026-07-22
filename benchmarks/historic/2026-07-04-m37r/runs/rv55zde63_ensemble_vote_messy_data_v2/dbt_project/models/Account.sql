-- models/Account.sql
{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account ' || account.id) AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(account.customer_tier__c) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(account.customer_tier__c) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(account.customer_tier__c) = 'bronze' THEN 'Bronze'
        WHEN LOWER(account.customer_tier__c) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    account.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account