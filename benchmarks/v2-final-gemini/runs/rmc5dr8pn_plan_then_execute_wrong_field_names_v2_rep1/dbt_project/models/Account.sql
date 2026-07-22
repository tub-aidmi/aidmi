-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    kunden.kunden_nr AS "Id",
    COALESCE(kunden.firmenname, 'UNSPECIFIED ACCOUNT') AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE
        WHEN kunden.kategorie ILIKE 'Gold' THEN 'Gold'
        WHEN kunden.kategorie ILIKE 'Silver' THEN 'Silver'
        WHEN kunden.kategorie ILIKE 'Bronze' THEN 'Bronze'
        WHEN kunden.kategorie ILIKE 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(kunden.gebiet)) AS "Region__c",
    INITCAP(TRIM(kunden.branche)) AS "Industry",
    kunden.webseite AS "Website",
    INITCAP(TRIM(kunden.ort)) AS "BillingCity",
    INITCAP(TRIM(kunden.land)) AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden