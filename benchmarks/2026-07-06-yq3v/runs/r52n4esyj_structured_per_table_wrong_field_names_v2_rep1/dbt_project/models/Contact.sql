-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), '') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    ap.funktion AS "Role__c",
    ap.sprache AS "Preferred_Language__c",
    TRIM(ap.kunde) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap