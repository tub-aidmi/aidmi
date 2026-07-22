-- depends_on: {{ source('fixture_master_v2_src', 'master_kunden') }}
{{ config(materialized='table') }}

SELECT
    MD5(master_kunden.kundennummer) AS "Id",
    TRIM(COALESCE(master_kunden.unternehmensname, master_kunden.kundennummer)) AS "Name",
    TRIM(master_kunden.erp_nr) AS "ERP_Number__c",
    CASE UPPER(TRIM(master_kunden.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
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