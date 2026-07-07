-- dbt model for the Account target table
-- depends_on: {{ source('fixture_messy_data_v2_src', 'account') }}

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'Unnamed Account') AS "Name",
    src.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(src.customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(src.customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(src.customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(src.customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    src.region__c AS "Region__c",
    src.industry AS "Industry",
    src.website AS "Website",
    src.billingcity AS "BillingCity",
    src.billingcountry AS "BillingCountry",
    src.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS src