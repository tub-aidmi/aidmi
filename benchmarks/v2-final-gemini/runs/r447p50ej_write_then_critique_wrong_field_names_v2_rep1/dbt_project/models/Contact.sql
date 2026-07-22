{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) IN ('endnutzer', 'end user') THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) IN ('technischer ansprechpartner', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('executive sponsor') THEN 'Executive Sponsor'
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
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    TRIM(ap.kunde) = TRIM(k.kunden_nr)
