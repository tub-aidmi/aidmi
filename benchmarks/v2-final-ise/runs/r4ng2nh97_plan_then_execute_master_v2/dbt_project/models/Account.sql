{{ config(materialized='table') }}

SELECT
    TRIM(kundennummer) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown') AS "Name",
    CASE WHEN TRIM(erp_nr) <> '' THEN TRIM(erp_nr) ELSE NULL END AS "ERP_Number__c",
    LOWER(TRIM(kundenklasse)) AS "Customer_Tier__c_raw",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    CASE
        WHEN TRIM(homepage) IS NULL OR TRIM(homepage) = '' THEN NULL
        WHEN TRIM(homepage) SIMILAR TO '(http://|https://).*' THEN LOWER(TRIM(homepage))
        ELSE 'https://' || TRIM(homepage)
    END AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
