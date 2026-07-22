-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Gold' THEN 'Gold'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Silver' THEN 'Silver'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Bronze' THEN 'Bronze'
        WHEN INITCAP(TRIM(customer_tier__c)) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}