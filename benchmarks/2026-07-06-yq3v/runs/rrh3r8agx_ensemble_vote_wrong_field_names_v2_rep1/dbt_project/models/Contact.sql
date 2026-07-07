{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, '') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('executive sponsor', 'führungskraft') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'GERMAN', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap