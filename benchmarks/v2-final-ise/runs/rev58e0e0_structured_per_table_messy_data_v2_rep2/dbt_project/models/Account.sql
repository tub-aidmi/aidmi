{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE LOWER(TRIM(customer_tier__c))
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'platin' THEN 'Platinum'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    CASE UPPER(TRIM(industry))
        WHEN 'FINANZEN' THEN 'Finance'
        WHEN 'GESUNDHEITSWESEN' THEN 'Healthcare'
        WHEN 'INDUSTRIE' THEN 'Manufacturing'
        WHEN 'TECHNOLOGIE' THEN 'Technology'
        ELSE INITCAP(TRIM(industry))
    END AS "Industry",
    website AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}