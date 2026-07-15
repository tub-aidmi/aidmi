{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unnamed Account') AS "Name",
    erp_number__c AS "ERP_Number__c",
    INITCAP(LOWER(customer_tier__c)) AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}