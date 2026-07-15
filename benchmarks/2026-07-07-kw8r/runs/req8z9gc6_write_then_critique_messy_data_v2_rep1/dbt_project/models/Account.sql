{{ config(materialized='table') }}
SELECT
    "fixture_messy_data_v2_src__account"."id" AS "Id",
    COALESCE(TRIM("fixture_messy_data_v2_src__account"."name"), 'Unknown') AS "Name",
    TRIM("fixture_messy_data_v2_src__account"."erp_number__c") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("fixture_messy_data_v2_src__account"."customer_tier__c")) IN ('PLATINUM', 'GOLD', 'SILVER', 'BRONZE')
        THEN INITCAP(LOWER(TRIM("fixture_messy_data_v2_src__account"."customer_tier__c")))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("fixture_messy_data_v2_src__account"."region__c") AS "Region__c",
    TRIM("fixture_messy_data_v2_src__account"."industry") AS "Industry",
    TRIM("fixture_messy_data_v2_src__account"."website") AS "Website",
    TRIM("fixture_messy_data_v2_src__account"."billingcity") AS "BillingCity",
    TRIM("fixture_messy_data_v2_src__account"."billingcountry") AS "BillingCountry",
    TRIM("fixture_messy_data_v2_src__account"."legacy_customer_id__c") AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }} AS "fixture_messy_data_v2_src__account"