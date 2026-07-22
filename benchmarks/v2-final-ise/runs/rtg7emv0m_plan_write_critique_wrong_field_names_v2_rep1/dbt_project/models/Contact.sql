{{ config(materialized='table') }}

SELECT 
    CAST(ap_id AS TEXT) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(nachname)), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    CASE 
        WHEN telefonnummer IS NOT NULL THEN REGEXP_REPLACE(telefonnummer, '[^0-9+]', '', 'g')
        ELSE NULL 
    END AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(funktion)) IN ('entscheider', 'decision maker', 'dm') THEN 'Decision Maker'
        WHEN LOWER(TRIM(funktion)) IN ('endanwender', 'user', 'end user') THEN 'End User'
        WHEN LOWER(TRIM(funktion)) LIKE '%technisch%' OR LOWER(TRIM(funktion)) IN ('technical contact', 'tc') THEN 'Technical Contact'
        WHEN LOWER(TRIM(funktion)) LIKE '%sponsor%' OR LOWER(TRIM(funktion)) IN ('executive sponsor', 'es') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'GERMAN', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(sprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(sprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(sprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(UPPER(kunde)) AS "AccountId",
    CAST(ap_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}