{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(customer_tier__c)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    CASE
        WHEN UPPER(TRIM(industry)) IN ('GESUNDHEITSWESEN', 'HEALTHCARE') THEN 'Healthcare'
        WHEN UPPER(TRIM(industry)) IN ('FINANZEN', 'FINANCE') THEN 'Finance'
        WHEN UPPER(TRIM(industry)) IN ('TECHNOLOGIE', 'TECHNOLOGY') THEN 'Technology'
        WHEN UPPER(TRIM(industry)) = 'INDUSTRIE' THEN 'Manufacturing'
        ELSE INITCAP(TRIM(industry))
    END AS "Industry",
    TRIM(website) AS "Website",
    TRIM(billingcity) AS "BillingCity",
    TRIM(billingcountry) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}