-- This dbt model transforms data from the master_kunden source table into the Account target schema.
-- It generates a Salesforce-style Id, maps customer attributes, and handles enum type conversions.

{{ config(materialized='table') }}

SELECT
    MD5(mk.kundennummer) AS "Id",
    COALESCE(TRIM(mk.unternehmensname), 'Unknown Account Name') AS "Name",
    TRIM(mk.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(mk.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(mk.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(mk.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(mk.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(mk.vertriebsgebiet) AS "Region__c",
    TRIM(mk.industrie) AS "Industry",
    TRIM(mk.homepage) AS "Website",
    TRIM(mk.stadt) AS "BillingCity",
    TRIM(mk.land_region) AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- No direct source, assuming system generation or NULL as per target spec
    NULL AS "LastModifiedDate", -- No direct source, assuming system generation or NULL as per target spec
    0 AS "IsDeleted" -- Default to not deleted as per common Salesforce practice
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk