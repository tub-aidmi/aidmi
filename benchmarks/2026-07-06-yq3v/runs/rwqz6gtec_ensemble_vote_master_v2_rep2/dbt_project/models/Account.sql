{{ config(materialized='table') }}

SELECT
    MD5(k.kundennummer)::text AS "Id",
    COALESCE(TRIM(k.unternehmensname), 'Unknown Account') AS "Name",
    TRIM(k.erp_nr) AS "ERP_Number__c",
    CASE LOWER(TRIM(k.kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k.vertriebsgebiet) AS "Region__c",
    TRIM(k.industrie) AS "Industry",
    TRIM(k.homepage) AS "Website",
    TRIM(k.stadt) AS "BillingCity",
    TRIM(k.land_region) AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.US') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.US') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
