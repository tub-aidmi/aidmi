{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'enduser') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technicalcontact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'executivesponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'GERMAN', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRENCH', 'FRANCAIS', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'SPANISH', 'CASTELLANO') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}