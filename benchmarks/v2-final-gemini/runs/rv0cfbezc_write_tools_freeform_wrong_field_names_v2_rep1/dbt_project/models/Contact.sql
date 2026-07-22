{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN LOWER(funktion) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(funktion) = 'end user' THEN 'End User'
        WHEN LOWER(funktion) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(sprache) = 'de' THEN 'DE'
        WHEN LOWER(sprache) = 'en' THEN 'EN'
        WHEN LOWER(sprache) = 'fr' THEN 'FR'
        WHEN LOWER(sprache) = 'es' THEN 'ES'
        WHEN LOWER(sprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId", -- Maps to Account.Id (kunden_nr)
    ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
