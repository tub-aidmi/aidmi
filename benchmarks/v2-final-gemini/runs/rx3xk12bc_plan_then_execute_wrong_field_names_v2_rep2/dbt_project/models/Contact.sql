{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    ap.nachname AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
