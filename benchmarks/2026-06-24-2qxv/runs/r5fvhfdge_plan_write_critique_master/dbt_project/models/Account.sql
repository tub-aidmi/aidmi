
{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), kundennummer) AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }}
