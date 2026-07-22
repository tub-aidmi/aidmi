{{ config(materialized='table') }}

SELECT
    CONCAT('A0', RIGHT('0000000000' || kunden_nr, 10)) AS "Id",
    TRIM(firmenname) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        WHEN 'premium' THEN 'Gold'
        WHEN 'basic' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
WHERE TRIM(firmenname) IS NOT NULL AND TRIM(firmenname) != ''