{{ config(materialized='table') }}
SELECT
    '001' || REPLACE(kunden_nr, 'CUST-', '') AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE WHEN UPPER(TRIM(kategorie)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(TRIM(kategorie)) ELSE NULL END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}