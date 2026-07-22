-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    MD5(t1.kundennummer) AS "Id",
    COALESCE(TRIM(t1.unternehmensname), 'Unknown Account') AS "Name",
    TRIM(t1.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(t1.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(t1.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(t1.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(t1.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(t1.vertriebsgebiet) AS "Region__c",
    TRIM(t1.industrie) AS "Industry",
    TRIM(t1.homepage) AS "Website",
    TRIM(t1.stadt) AS "BillingCity",
    TRIM(t1.land_region) AS "BillingCountry",
    TRIM(t1.kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS t1