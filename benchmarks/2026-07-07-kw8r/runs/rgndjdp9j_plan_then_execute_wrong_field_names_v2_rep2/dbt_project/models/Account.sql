{{ config(materialized='table') }}

SELECT
    CONCAT('a00', TRIM(kunden_nr)) AS "Id",
    CASE 
        WHEN TRIM(firmenname) IS NOT NULL AND TRIM(firmenname) != '' THEN INITCAP(TRIM(firmenname))
        ELSE 'Unknown Customer'
    END AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kategorie)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kategorie)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kategorie)) = 'platin' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    LOWER(TRIM(webseite)) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}