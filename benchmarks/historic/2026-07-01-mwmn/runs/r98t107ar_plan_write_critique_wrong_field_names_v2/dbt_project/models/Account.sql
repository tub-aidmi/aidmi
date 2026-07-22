{{ config(materialized='table') }}
SELECT
    LEFT(MD5(TRIM(k."kunden_nr")), 18) AS "Id",
    INITCAP(TRIM(k."firmenname")) AS "Name",
    TRIM(k."erp_nummer") AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(k."kategorie")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM')
        THEN INITCAP(TRIM(k."kategorie"))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k."gebiet") AS "Region__c",
    TRIM(k."branche") AS "Industry",
    CASE
        WHEN TRIM(k."webseite") IS NOT NULL AND TRIM(k."webseite") != ''
        THEN REGEXP_REPLACE(TRIM(k."webseite"), '^http://', 'https://', 'g')
        ELSE NULL
    END AS "Website",
    TRIM(k."ort") AS "BillingCity",
    TRIM(k."land") AS "BillingCountry",
    TRIM(k."kunden_nr") AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k