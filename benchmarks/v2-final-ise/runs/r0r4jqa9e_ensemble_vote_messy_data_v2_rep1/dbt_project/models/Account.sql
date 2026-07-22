{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
    CASE LOWER(TRIM(customer_tier__c))
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'gold' THEN 'Gold'
        WHEN 'platin' THEN 'Platinum'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'silber' THEN 'Silver'
        WHEN 'silver' THEN 'Silver'
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(region__c AS TEXT) AS "Region__c",
    CASE UPPER(TRIM(industry))
        WHEN 'FINANCE' THEN 'Finance'
        WHEN 'FINANZEN' THEN 'Finance'
        WHEN 'GESUNDHEITSWESEN' THEN 'Healthcare'
        WHEN 'HEALTHCARE' THEN 'Healthcare'
        WHEN 'INDUSTRIE' THEN 'Manufacturing'
        WHEN 'IT' THEN 'IT'
        WHEN 'MANUFACTURING' THEN 'Manufacturing'
        WHEN 'TECHNOLOGIE' THEN 'Technology'
        WHEN 'TECHNOLOGY' THEN 'Technology'
        ELSE industry
    END AS "Industry",
    CAST(website AS TEXT) AS "Website",
    CAST(billingcity AS TEXT) AS "BillingCity",
    CAST(billingcountry AS TEXT) AS "BillingCountry",
    CAST(id AS TEXT) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}