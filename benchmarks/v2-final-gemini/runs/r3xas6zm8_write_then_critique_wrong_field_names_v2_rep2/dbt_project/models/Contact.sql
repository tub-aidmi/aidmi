-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN UPPER(TRIM(ap.funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
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
    TRIM(k.kunden_nr) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)