-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    TRIM(m.kundennummer) AS "Id",
    COALESCE(TRIM(m.unternehmensname), TRIM(m.kundennummer)) AS "Name",
    TRIM(m.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(m.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(m.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(m.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(m.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(m.vertriebsgebiet) AS "Region__c",
    TRIM(m.industrie) AS "Industry",
    TRIM(m.homepage) AS "Website",
    TRIM(m.stadt) AS "BillingCity",
    TRIM(m.land_region) AS "BillingCountry",
    TRIM(m.kundennummer) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS m