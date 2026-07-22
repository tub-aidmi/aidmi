{{ config(materialized='table') }}

SELECT
    kunden.kunden_nr AS "Id",
    COALESCE(TRIM(INITCAP(kunden.firmenname)), kunden.kunden_nr) AS "Name",
    TRIM(kunden.erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kunden.kategorie)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.gebiet) AS "Region__c",
    TRIM(kunden.branche) AS "Industry",
    TRIM(kunden.webseite) AS "Website",
    TRIM(kunden.ort) AS "BillingCity",
    TRIM(kunden.land) AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
