{{ config(materialized='table') }}

SELECT
    MD5(kunden.kunden_nr) AS "Id",
    COALESCE(INITCAP(TRIM(kunden.firmenname)), 'Unknown Account') AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE LOWER(TRIM(kunden.kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.gebiet AS "Region__c",
    kunden.branche AS "Industry",
    kunden.webseite AS "Website",
    kunden.ort AS "BillingCity",
    kunden.land AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
