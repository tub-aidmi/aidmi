{{ config(materialized='table') }}

SELECT 
    kundennummer AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'goldkategorie', 'goldkunde', 'premium', 'top') THEN 'Platinum'
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'gold-tier', 'goldkategor') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silber', 'silver', 'silber-kategorie', 'silberkunde') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', 'bronze-kategorie', 'bronzekunde', 'basis') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    TRIM(industrie) AS "Industry",
    CASE 
        WHEN TRIM(homepage) IS NOT NULL AND TRIM(homepage) != '' THEN
            CASE 
                WHEN LOWER(TRIM(homepage)) LIKE 'https://%' OR LOWER(TRIM(homepage)) LIKE 'http://%' 
                    THEN TRIM(homepage)
                WHEN LOWER(TRIM(homepage)) LIKE 'www.%' 
                    THEN 'https://' || TRIM(homepage)
                ELSE 'https://' || TRIM(homepage)
            END
        ELSE NULL
    END AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}