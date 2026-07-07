{{
  config(materialized='table')
}}

SELECT
    MD5(TRIM(kundennummer)) AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account Name') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}
