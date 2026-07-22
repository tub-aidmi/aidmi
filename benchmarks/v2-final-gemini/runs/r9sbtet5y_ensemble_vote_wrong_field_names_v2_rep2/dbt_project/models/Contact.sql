{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(ap.nachname)), '') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) = 'endbenutzer' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) = 'technischer ansprechpartner' THEN 'Technical Contact'
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
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
