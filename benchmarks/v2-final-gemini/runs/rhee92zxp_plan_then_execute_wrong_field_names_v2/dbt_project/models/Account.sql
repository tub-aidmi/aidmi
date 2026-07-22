{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        kunden_nr,
        firmenname,
        erp_nummer,
        kategorie,
        gebiet,
        branche,
        webseite,
        ort,
        land
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
    SHA256(source_data.kunden_nr) AS "Id",
    TRIM(source_data.firmenname) AS "Name",
    source_data.erp_nummer AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(source_data.kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(source_data.kategorie)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(source_data.kategorie)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(source_data.kategorie)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    source_data.gebiet AS "Region__c",
    source_data.branche AS "Industry",
    source_data.webseite AS "Website",
    source_data.ort AS "BillingCity",
    source_data.land AS "BillingCountry",
    source_data.kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data