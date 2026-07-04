{{ config(materialized='table') }}

SELECT
    MD5(TRIM(master_kunden.kundennummer)) AS "Id",
    COALESCE(TRIM(INITCAP(master_kunden.unternehmensname)), 'Unnamed Account') AS "Name",
    TRIM(master_kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(master_kunden.vertriebsgebiet) AS "Region__c",
    TRIM(master_kunden.industrie) AS "Industry",
    LOWER(TRIM(master_kunden.homepage)) AS "Website",
    TRIM(INITCAP(master_kunden.stadt)) AS "BillingCity",
    TRIM(INITCAP(master_kunden.land_region)) AS "BillingCountry",
    TRIM(master_kunden.kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS master_kunden
