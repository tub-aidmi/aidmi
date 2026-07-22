{{ config(materialized='table') }}

SELECT
    MD5(TRIM(mk.kundennummer)) AS "Id",
    TRIM(COALESCE(mk.unternehmensname, mk.kundennummer)) AS "Name",
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
    TRIM(mk.kundennummer) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
