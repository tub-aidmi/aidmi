{{ config(materialized='table') }}

SELECT
    id AS "Id",
    NULLIF(TRIM(firstname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(email), '') AS "Email",
    NULLIF(TRIM(phone), '') AS "Phone",
    NULLIF(TRIM(title), '') AS "Title",
    CASE 
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'dm', 'decision_maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'eu', 'end_user') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'tc', 'tech_contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'es', 'exec_sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'DEU', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENG', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRA', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'ESP', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITA', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
