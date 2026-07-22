{{ config(materialized='table') }}

SELECT
    MD5(master_kunden.kundennummer) AS "Id",
    COALESCE(TRIM(master_kunden.unternehmensname), 'N/A') AS "Name",
    TRIM(master_kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN master_kunden.kundenklasse ILIKE 'Gold' THEN 'Gold'
        WHEN master_kunden.kundenklasse ILIKE 'Silver' THEN 'Silver'
        WHEN master_kunden.kundenklasse ILIKE 'Bronze' THEN 'Bronze'
        WHEN master_kunden.kundenklasse ILIKE 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(master_kunden.vertriebsgebiet) AS "Region__c",
    TRIM(master_kunden.industrie) AS "Industry",
    TRIM(master_kunden.homepage) AS "Website",
    TRIM(master_kunden.stadt) AS "BillingCity",
    TRIM(master_kunden.land_region) AS "BillingCountry",
    master_kunden.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS master_kunden
