{{ config(materialized='table') }}

SELECT
    TRIM(kunden.kunden_nr) AS "Id",
    COALESCE(TRIM(kunden.firmenname), 'Unknown Account ' || TRIM(kunden.kunden_nr)) AS "Name",
    TRIM(kunden.erp_nummer) AS "ERP_Number__c",
    CASE UPPER(TRIM(kunden.kategorie))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.gebiet) AS "Region__c",
    TRIM(kunden.branche) AS "Industry",
    TRIM(kunden.webseite) AS "Website",
    TRIM(kunden.ort) AS "BillingCity",
    TRIM(kunden.land) AS "BillingCountry",
    TRIM(kunden.kunden_nr) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
