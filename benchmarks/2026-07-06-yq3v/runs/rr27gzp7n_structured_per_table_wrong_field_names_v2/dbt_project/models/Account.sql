-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    kunden.kunden_nr AS "Id",
    COALESCE(kunden.firmenname, kunden.kunden_nr) AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    CASE
        WHEN INITCAP(kunden.kategorie) = 'Gold' THEN 'Gold'
        WHEN INITCAP(kunden.kategorie) = 'Silver' THEN 'Silver'
        WHEN INITCAP(kunden.kategorie) = 'Bronze' THEN 'Bronze'
        WHEN INITCAP(kunden.kategorie) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.gebiet AS "Region__c",
    kunden.branche AS "Industry",
    kunden.webseite AS "Website",
    kunden.ort AS "BillingCity",
    kunden.land AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden