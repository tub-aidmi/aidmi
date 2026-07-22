-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    MD5(kunden.kunden_nr) AS "Id",
    COALESCE(kunden.firmenname, '') AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE UPPER(TRIM(kunden.kategorie))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.gebiet AS "Region__c",
    kunden.branche AS "Industry",
    kunden.webseite AS "Website",
    kunden.ort AS "BillingCity",
    kunden.land AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
