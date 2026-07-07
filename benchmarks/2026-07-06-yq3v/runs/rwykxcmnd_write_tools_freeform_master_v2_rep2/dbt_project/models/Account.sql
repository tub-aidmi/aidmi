{{ config(materialized='table') }}

WITH cleaned_kunden AS (
    SELECT
        kundennummer,
        unternehmensname,
        erp_nr,
        kundenklasse,
        vertriebsgebiet,
        industrie,
        homepage,
        stadt,
        land_region,
        -- Defaulting CreatedDate and LastModifiedDate as source doesn't provide
        CAST(CURRENT_TIMESTAMP AS TEXT) AS created_date,
        CAST(CURRENT_TIMESTAMP AS TEXT) AS last_modified_date
    FROM
        {{ source('fixture_master_v2_src', 'master_kunden') }}
)
SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(unternehmensname, 'Unknown Account') AS "Name", -- Name is NOT NULL
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kundenklasse) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    created_date AS "CreatedDate",
    last_modified_date AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_kunden
