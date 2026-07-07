{{ config(materialized='table') }}

SELECT
    MD5(TRIM(mk.kundennummer)) AS "Id",
    COALESCE(TRIM(mk.unternehmensname), 'Unknown Account Name') AS "Name",
    TRIM(mk.erp_nr) AS "ERP_Number__c",
    CASE UPPER(TRIM(mk.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(mk.vertriebsgebiet) AS "Region__c",
    TRIM(mk.industrie) AS "Industry",
    TRIM(mk.homepage) AS "Website",
    TRIM(mk.stadt) AS "BillingCity",
    TRIM(mk.land_region) AS "BillingCountry",
    TRIM(mk.kundennummer) AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
WHERE
    mk.kundennummer IS NOT NULL
