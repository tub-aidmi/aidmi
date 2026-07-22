{{ config(materialized='table') }}

SELECT
    MD5(kunden_nr) AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN LOWER(kategorie) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(kategorie) IN ('silber', 'silver') THEN 'Silver'
        WHEN LOWER(kategorie) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(kategorie) IN ('platin', 'platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
