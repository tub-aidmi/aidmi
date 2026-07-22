{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kunde.kundennummer)) AS "Id",
    TRIM(kunde.unternehmensname) AS "Name",
    TRIM(kunde.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kunde.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kunde.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kunde.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kunde.kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunde.vertriebsgebiet) AS "Region__c",
    TRIM(kunde.industrie) AS "Industry",
    TRIM(kunde.homepage) AS "Website",
    TRIM(kunde.stadt) AS "BillingCity",
    TRIM(kunde.land_region) AS "BillingCountry",
    TRIM(kunde.kundennummer) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunde
WHERE
    kunde.kundennummer IS NOT NULL
AND
    kunde.unternehmensname IS NOT NULL