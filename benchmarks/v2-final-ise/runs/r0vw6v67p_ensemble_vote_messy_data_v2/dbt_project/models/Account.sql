{{ config(materialized='table') }}

SELECT
    -- Transform source id to Salesforce-style Account Id (001 prefix + padded)
    CAST('001' || LPAD(REPLACE(id, 'CUST-', ''), 12, '0') AS text) AS "Id",
    -- Name: use default for nulls to satisfy NOT NULL constraint
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    erp_number__c AS "ERP_Number__c",
    -- Map customer_tier values (including German variants) to enum domain
    CASE LOWER(TRIM(customer_tier__c))
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'silber' THEN 'Silver'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}