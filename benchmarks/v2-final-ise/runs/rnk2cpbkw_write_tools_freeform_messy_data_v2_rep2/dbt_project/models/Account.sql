{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        erp_number__c,
        customer_tier__c,
        region__c,
        industry,
        website,
        billingcity,
        billingcountry,
        legacy_customer_id__c
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),

transformed AS (
    SELECT
        id AS "Id",
        COALESCE(TRIM(INITCAP(name)), '') AS "Name",
        CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
        CASE
            WHEN LOWER(TRIM(customer_tier__c)) IN ('gold') THEN 'Gold'
            WHEN LOWER(TRIM(customer_tier__c)) IN ('silver') THEN 'Silver'
            WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze') THEN 'Bronze'
            WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum') THEN 'Platinum'
            ELSE NULL
        END AS "Customer_Tier__c",
        TRIM(INITCAP(region__c)) AS "Region__c",
        TRIM(INITCAP(industry)) AS "Industry",
        TRIM(LOWER(website)) AS "Website",
        TRIM(billingcity) AS "BillingCity",
        TRIM(billingcountry) AS "BillingCountry",
        COALESCE(erp_number__c, id) AS "Legacy_Customer_ID__c",
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM source_data
)

SELECT * FROM transformed
