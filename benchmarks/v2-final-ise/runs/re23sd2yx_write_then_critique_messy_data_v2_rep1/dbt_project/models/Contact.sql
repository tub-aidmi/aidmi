{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(COALESCE(firstname, '')) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    CASE
        WHEN LOWER(TRIM(email)) IN ('', 'n/a') THEN NULL
        ELSE LOWER(TRIM(email))
    END AS "Email",
    CASE
        WHEN TRIM(LOWER(phone)) IN ('n/a', '') THEN NULL
        ELSE TRIM(phone)
    END AS "Phone",
    COALESCE(INITCAP(TRIM(title)), '') AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'sponsor', 'entscheider') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}