{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(NULLIF(name, ''), 'Unknown'))) AS "Name",
    CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
    CASE UPPER(TRIM(COALESCE(customer_tier__c, '')))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'PLATIN' THEN 'Platinum'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(region__c AS TEXT) AS "Region__c",
    CAST(industry AS TEXT) AS "Industry",
    CAST(website AS TEXT) AS "Website",
    CAST(billingcity AS TEXT) AS "BillingCity",
    CAST(billingcountry AS TEXT) AS "BillingCountry",
    CAST(id AS TEXT) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}