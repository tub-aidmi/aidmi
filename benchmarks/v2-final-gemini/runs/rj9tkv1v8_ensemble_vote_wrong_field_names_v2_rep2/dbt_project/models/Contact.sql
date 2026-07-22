{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(INITCAP(ap.vorname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(ap.nachname)), '(Unknown)') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) IN ('geschäftsführer', 'ceo', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) IN ('end user', 'benutzer', 'nutzer') THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) IN ('techniker', 'technical contact', 'it support') THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('executive sponsor', 'vorstand', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(ap.sprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(ap.sprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(ap.sprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(ap.sprache)) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(ap.sprache)) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(ap.kunde) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
