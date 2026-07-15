{{ config(materialized='table') }}

SELECT
    MD5(kunden_nr)::TEXT AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN kategorie IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN kategorie
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}