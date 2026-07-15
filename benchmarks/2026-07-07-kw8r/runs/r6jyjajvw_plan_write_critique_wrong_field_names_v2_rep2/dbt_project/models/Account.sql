{{ config(materialized='table') }}

SELECT
    CAST(kunden_nr AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(firmenname)), 'Unknown') AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE TRIM(COALESCE(kategorie, ''))
        WHEN 'Gold'          THEN 'Gold'
        WHEN 'Silber'        THEN 'Silver'
        WHEN 'Bronze'        THEN 'Bronze'
        WHEN 'Platin'        THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    CASE
        WHEN TRIM(webseite) IS NULL OR TRIM(webseite) = '' THEN NULL
        WHEN TRIM(webseite) ~ '^https?://'                 THEN LOWER(TRIM(webseite))
        ELSE 'https://' || LOWER(TRIM(webseite))
    END AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    UPPER(TRIM(land)) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}