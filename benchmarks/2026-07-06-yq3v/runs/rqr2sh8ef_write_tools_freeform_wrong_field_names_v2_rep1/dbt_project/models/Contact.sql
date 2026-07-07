{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, ap_id) AS "LastName", -- LastName is NOT NULL, using ap_id as fallback
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    funktion AS "Role__c", -- Matches enum values directly
    sprache AS "Preferred_Language__c", -- Matches enum values directly
    kunde AS "AccountId", -- Corresponds to Account.Id (kunden_nr)
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
