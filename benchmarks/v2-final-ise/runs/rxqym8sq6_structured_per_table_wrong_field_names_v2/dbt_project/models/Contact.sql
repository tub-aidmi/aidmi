{{ config(materialized='table') }}

SELECT
    CAST(ap_id AS TEXT) AS "Id",
    vorname AS "FirstName",
    nachname AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    funktion AS "Role__c",
    sprache AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}