{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(name, '') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE LOWER(TRIM(COALESCE(customer_tier__c, '')))
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'gold' THEN 'Gold'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'silver' THEN 'Silver'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(region__c) AS "Region__c",
    INITCAP(industry) AS "Industry",
    website AS "Website",
    INITCAP(billingcity) AS "BillingCity",
    INITCAP(billingcountry) AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    CAST('' AS TEXT) AS "CreatedDate",
    CAST('' AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
