{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
