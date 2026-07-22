{{ config(materialized='table') }}

SELECT
    MD5(kunden.kunden_nr) AS "Id",
    COALESCE(TRIM(kunden.firmenname), 'Unknown Account Name') AS "Name",
    TRIM(kunden.erp_nummer) AS "ERP_Number__c",
    CASE TRIM(LOWER(kunden.kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.gebiet) AS "Region__c",
    TRIM(kunden.branche) AS "Industry",
    TRIM(LOWER(kunden.webseite)) AS "Website",
    TRIM(kunden.ort) AS "BillingCity",
    TRIM(kunden.land) AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
