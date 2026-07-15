{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    TRIM(vorname) AS "FirstName",
    TRIM(nachname) AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE 
        WHEN LOWER(TRIM(funktion)) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(funktion)) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN LOWER(TRIM(funktion)) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(funktion)) IN ('geschäftsführung', 'executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(sprache)) IN ('deutsch', 'german', 'de') THEN 'DE'
        WHEN LOWER(TRIM(sprache)) IN ('englisch', 'english', 'en') THEN 'EN'
        WHEN LOWER(TRIM(sprache)) IN ('französisch', 'french', 'fr') THEN 'FR'
        WHEN LOWER(TRIM(sprache)) IN ('spanisch', 'spanish', 'es') THEN 'ES'
        WHEN LOWER(TRIM(sprache)) IN ('italienisch', 'italian', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
