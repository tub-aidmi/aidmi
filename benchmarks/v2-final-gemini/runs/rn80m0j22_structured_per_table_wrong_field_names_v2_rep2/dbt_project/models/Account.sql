{{ config(materialized='table') }}

SELECT
    TRIM(kunden_nr) AS "Id",
    TRIM(firmenname) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE TRIM(kategorie)
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silver' THEN 'Silver'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    TRIM(branche) AS "Industry",
    TRIM(webseite) AS "Website",
    TRIM(ort) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
