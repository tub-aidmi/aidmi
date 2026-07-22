{{ config(materialized='table') }}

SELECT
    '001' || MD5(kunden_nr) AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN UPPER(kategorie) = 'GOLD' THEN 'Gold'
        WHEN UPPER(kategorie) = 'SILVER' THEN 'Silver'
        WHEN UPPER(kategorie) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(kategorie) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    INITCAP(branche) AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
