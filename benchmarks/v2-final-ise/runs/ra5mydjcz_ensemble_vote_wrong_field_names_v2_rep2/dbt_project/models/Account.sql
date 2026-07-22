{{ config(materialized='table') }}

SELECT
    'ACCT_' || k."kunden_nr" AS "Id",
    TRIM(k."firmenname") AS "Name",
    TRIM(k."erp_nummer") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(k."kategorie")) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(k."kategorie")) IN ('GOLD', 'PREMIUM') THEN 'Gold'
        WHEN UPPER(TRIM(k."kategorie")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(k."kategorie")) IN ('BRONZE', 'STANDARD') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k."gebiet") AS "Region__c",
    TRIM(k."branche") AS "Industry",
    TRIM(k."webseite") AS "Website",
    TRIM(k."ort") AS "BillingCity",
    TRIM(k."land") AS "BillingCountry",
    k."kunden_nr" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k