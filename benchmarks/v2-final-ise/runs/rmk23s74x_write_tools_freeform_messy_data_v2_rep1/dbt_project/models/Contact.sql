{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'dm', 'dmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'eu', 'user') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'tc', 'tech contact', 'technical') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'es', 'sponsor', 'executive') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'GER', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENG', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRA', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'ESP', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITA', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
