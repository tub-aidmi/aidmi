{{ config(materialized='table') }}

SELECT
    LOWER(SUBSTR(MD5('acc_' || kunden_nr), 1, 15)) AS "Id",
    COALESCE(firmenname, 'Unknown') AS "Name",
    erp_nummer AS "ERP_Number__c",
    kategorie AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}