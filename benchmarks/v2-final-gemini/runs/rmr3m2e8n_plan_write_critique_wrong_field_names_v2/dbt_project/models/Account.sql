{{ config(materialized='table') }}

SELECT
    kunden.kunden_nr AS "Id",
    COALESCE(TRIM(kunden.firmenname), kunden.kunden_nr) AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE TRIM(UPPER(kunden.kategorie))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.gebiet AS "Region__c",
    kunden.branche AS "Industry",
    TRIM(kunden.webseite) AS "Website",
    kunden.ort AS "BillingCity",
    kunden.land AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
