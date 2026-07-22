{{ config(materialized='table') }}

SELECT
    UPPER(SUBSTRING(MD5(LOWER(kunden_nr::text)) FROM 1 FOR 18)) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE LOWER(TRIM(COALESCE(kategorie, '')))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    webseite AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
