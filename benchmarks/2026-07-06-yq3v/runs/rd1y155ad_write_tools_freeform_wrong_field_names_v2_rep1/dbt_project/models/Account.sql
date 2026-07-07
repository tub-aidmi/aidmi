{{ config(materialized='table') }}

SELECT
    kunden_nr AS "Id",
    COALESCE(firmenname, 'N/A') AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE
        WHEN LOWER(kategorie) IN ('gold', 'gold customer') THEN 'Gold'
        WHEN LOWER(kategorie) IN ('silver', 'silver customer') THEN 'Silver'
        WHEN LOWER(kategorie) IN ('bronze', 'bronze customer') THEN 'Bronze'
        WHEN LOWER(kategorie) IN ('platinum', 'platinum customer') THEN 'Platinum'
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
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
WHERE
    kunden_nr IS NOT NULL
