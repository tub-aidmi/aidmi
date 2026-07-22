{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), '') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(ap.sprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(ap.sprache)) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(ap.sprache)) IN ('fr', 'francais', 'french') THEN 'FR'
        WHEN LOWER(TRIM(ap.sprache)) IN ('es', 'espanol', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(ap.sprache)) IN ('it', 'italiano', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(ap.kunde) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
