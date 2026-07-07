-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    nachname AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN funktion = 'End User' THEN 'End User'
        WHEN funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN sprache = 'DE' THEN 'DE'
        WHEN sprache = 'EN' THEN 'EN'
        WHEN sprache = 'FR' THEN 'FR'
        WHEN sprache = 'ES' THEN 'ES'
        WHEN sprache = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId", -- Assuming 'kunde' is the kunden_nr from the Account table
    ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
