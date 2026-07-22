-- models/Account.sql
{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account') AS "Name", -- Name is NOT NULL
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(kundenklasse) IN ('platin', 'platinum') THEN 'Platinum'
        WHEN LOWER(kundenklasse) IN ('gold') THEN 'Gold'
        WHEN LOWER(kundenklasse) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(kundenklasse) IN ('bronze') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- No source for CreatedDate
    NULL AS "LastModifiedDate", -- No source for LastModifiedDate
    0 AS "IsDeleted" -- Default to 0 (false)
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}
