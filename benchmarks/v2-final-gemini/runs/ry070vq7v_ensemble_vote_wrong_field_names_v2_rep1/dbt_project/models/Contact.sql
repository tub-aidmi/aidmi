{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    TRIM(LOWER(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) = 'anwender' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('geschäftsführer', 'vorstand') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(ap.sprache)) = 'deutsch' THEN 'DE'
        WHEN LOWER(TRIM(ap.sprache)) = 'english' THEN 'EN'
        WHEN LOWER(TRIM(ap.sprache)) = 'französisch' THEN 'FR'
        WHEN LOWER(TRIM(ap.sprache)) = 'spanisch' THEN 'ES'
        WHEN LOWER(TRIM(ap.sprache)) = 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    ap.kunde = k.kunden_nr
