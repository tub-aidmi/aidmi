{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(name, 'Unknown Account') AS "Name",
    CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    CASE
        WHEN industry IS NOT NULL THEN INITCAP(
            CASE LOWER(TRIM(industry))
                WHEN 'finanzen' THEN 'Finance'
                WHEN 'gesundheitswesen' THEN 'Healthcare'
                WHEN 'technologie' THEN 'Technology'
                WHEN 'industrie' THEN 'Industrial'
                ELSE industry
            END
        )
        ELSE NULL
    END AS "Industry",
    CAST(website AS TEXT) AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}