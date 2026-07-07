{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    nachname AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    -- Map source "funktion" to target "Role__c" enum values
    CASE
        WHEN funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN funktion
        ELSE NULL
    END AS "Role__c",
    -- Map source "sprache" to target "Preferred_Language__c" enum values
    CASE
        WHEN sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
