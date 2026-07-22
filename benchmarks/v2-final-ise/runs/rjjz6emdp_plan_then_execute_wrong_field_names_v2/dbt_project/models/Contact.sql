{{ config(materialized='table') }}

SELECT 
    TRIM(ap_id) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    INITCAP(TRIM(nachname)) AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(funktion)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(funktion)) IN ('end user', 'endanwender', 'benutzer') THEN 'End User'
        WHEN LOWER(TRIM(funktion)) IN ('technical contact', 'technisch', 'technical') THEN 'Technical Contact'
        WHEN LOWER(TRIM(funktion)) IN ('executive sponsor', 'vorstand', 'leitung', 'exekutiver') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sprache)) IN ('DEU', 'DE') THEN 'DE'
        WHEN UPPER(TRIM(sprache)) IN ('ENG', 'EN') THEN 'EN'
        WHEN UPPER(TRIM(sprache)) IN ('FRA', 'FR') THEN 'FR'
        WHEN UPPER(TRIM(sprache)) IN ('SPA', 'ES') THEN 'ES'
        WHEN UPPER(TRIM(sprache)) IN ('ITA', 'IT') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kunde) AS "AccountId",
    TRIM(ap_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}