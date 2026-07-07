-- depends_on: {{ ref("Account") }}

{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account ' || account.id) AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN TRIM(INITCAP(account.customer_tier__c)) = 'Gold' THEN 'Gold'
        WHEN TRIM(INITCAP(account.customer_tier__c)) = 'Silver' THEN 'Silver'
        WHEN TRIM(INITCAP(account.customer_tier__c)) = 'Bronze' THEN 'Bronze'
        WHEN TRIM(INITCAP(account.customer_tier__c)) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    COALESCE(account.legacy_customer_id__c, account.id) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account