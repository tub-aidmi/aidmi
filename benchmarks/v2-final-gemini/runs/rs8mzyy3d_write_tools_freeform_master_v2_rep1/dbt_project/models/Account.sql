-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kundennummer)) AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account') AS "Name", -- Name is NOT NULL
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}
WHERE
    kundennummer IS NOT NULL;
