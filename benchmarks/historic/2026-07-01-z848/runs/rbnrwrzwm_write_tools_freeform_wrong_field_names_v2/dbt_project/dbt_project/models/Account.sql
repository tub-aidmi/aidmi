{{ config(materialized='table') }}

SELECT
    kunden_nr AS "Id",
    COALESCE(firmenname, 'Unknown') AS "Name", -- Name is NOT NULL
    erp_nummer AS "ERP_Number__c",
    CASE
        WHEN TRIM(kategorie) = 'Gold' THEN 'Gold'
        WHEN TRIM(kategorie) = 'Silver' THEN 'Silver'
        WHEN TRIM(kategorie) = 'Bronze' THEN 'Bronze'
        WHEN TRIM(kategorie) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
