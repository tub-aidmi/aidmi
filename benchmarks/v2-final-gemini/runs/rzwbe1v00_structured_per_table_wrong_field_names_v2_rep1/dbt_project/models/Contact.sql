-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}

{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN TRIM(ap.funktion) = 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM(ap.funktion) = 'End User' THEN 'End User'
        WHEN TRIM(ap.funktion) = 'Technical Contact' THEN 'Technical Contact'
        WHEN TRIM(ap.funktion) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(ap.sprache) = 'DE' THEN 'DE'
        WHEN TRIM(ap.sprache) = 'EN' THEN 'EN'
        WHEN TRIM(ap.sprache) = 'FR' THEN 'FR'
        WHEN TRIM(ap.sprache) = 'ES' THEN 'ES'
        WHEN TRIM(ap.sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(ap.kunde) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap