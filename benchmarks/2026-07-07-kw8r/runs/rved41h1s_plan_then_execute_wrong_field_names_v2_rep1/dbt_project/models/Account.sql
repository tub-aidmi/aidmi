{{ config(materialized='table') }}

SELECT 
    TRIM(kunden_nr) AS "Id",
    COALESCE(TRIM(firmenname), 'Unknown') AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kategorie)) IN ('PREMIUM', 'VIP') THEN 'Platinum'
        WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) IN ('STANDARD', 'REGULAR') THEN 'Silver'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    CASE 
        WHEN NULLIF(TRIM(webseite), '') IS NOT NULL THEN 
            CASE 
                WHEN TRIM(webseite) ~ '^https?://' THEN TRIM(webseite)
                ELSE 'https://' || TRIM(webseite)
            END
        ELSE NULL 
    END AS "Website",
    TRIM(ort) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}