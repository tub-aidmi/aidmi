{{ config(materialized='table') }}

SELECT
    'A' || LEFT(MD5(kundennummer), 14) AS "Id",
    INITCAP(TRIM(COALESCE(unternehmensname, ''))) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE INITCAP(TRIM(COALESCE(kundenklasse, '')))
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silver' THEN 'Silver'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
