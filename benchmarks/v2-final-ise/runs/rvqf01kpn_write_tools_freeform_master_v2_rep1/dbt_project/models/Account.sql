{{ config(materialized='table') }}

SELECT
     '001' || LPAD(CAST(REGEXP_REPLACE(kundennummer, '\D', '', 'g') AS TEXT), 9, '0') AS "Id",
    INITCAP(TRIM(unternehmensname)) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
