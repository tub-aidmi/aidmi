{{ config(materialized='table') }}

SELECT
    CONCAT('001', LEFT(MD5(UPPER(TRIM(kunden_nr))), 12)) AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE INITCAP(TRIM(kategorie))
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silber' THEN 'Silver'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branchE AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}