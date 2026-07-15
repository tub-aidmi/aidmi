{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(LOWER(TRIM(name)), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'silver', 'bronze', 'platinum') 
            THEN INITCAP(TRIM(customer_tier__c))
        ELSE NULL 
    END AS "Customer_Tier__c",
    UPPER(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    UPPER(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    CAST(COALESCE(NULLIF('', ''::TEXT), CURRENT_TIMESTAMP::TEXT) AS TEXT) AS "CreatedDate",
    CAST(COALESCE(NULLIF('', ''::TEXT), CURRENT_TIMESTAMP::TEXT) AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'account') }}
