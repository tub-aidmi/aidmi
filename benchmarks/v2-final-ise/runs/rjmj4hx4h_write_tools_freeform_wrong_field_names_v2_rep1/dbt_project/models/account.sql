{{ config(materialized='table') }}

SELECT 
    SUBSTRING(MD5(kunden_nr), 1, 18) AS "Id",
    TRIM(firmenname) AS "Name",
    erp_nummer AS "ERP_Number__c",
    kategorie AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    INITCAP(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
