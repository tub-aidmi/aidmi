{{ config(materialized='table') }}

SELECT
    TRIM(kunden_nr) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    CAST(erp_nummer AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kategorie)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM')
        THEN INITCAP(UPPER(TRIM(kategorie)))
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(gebiet AS TEXT) AS "Region__c",
    CAST(branche AS TEXT) AS "Industry",
    CAST(webseite AS TEXT) AS "Website",
    CAST(ort AS TEXT) AS "BillingCity",
    CAST(land AS TEXT) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}