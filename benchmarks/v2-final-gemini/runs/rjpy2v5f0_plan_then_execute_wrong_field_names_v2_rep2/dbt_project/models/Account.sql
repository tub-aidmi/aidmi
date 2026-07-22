{{ config(materialized='table') }}

SELECT
    MD5(kunden.kunden_nr) AS "Id",
    COALESCE(INITCAP(TRIM(kunden.firmenname)), 'Unknown Account') AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE
        WHEN LOWER(kunden.kategorie) = 'premium' THEN 'Gold'
        WHEN LOWER(kunden.kategorie) = 'standard' THEN 'Silver'
        WHEN LOWER(kunden.kategorie) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kunden.kategorie) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.gebiet) AS "Region__c",
    INITCAP(TRIM(kunden.branche)) AS "Industry",
    LOWER(TRIM(kunden.webseite)) AS "Website",
    INITCAP(TRIM(kunden.ort)) AS "BillingCity",
    INITCAP(TRIM(kunden.land)) AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
