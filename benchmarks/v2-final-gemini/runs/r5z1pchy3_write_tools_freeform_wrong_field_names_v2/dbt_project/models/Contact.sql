-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    INITCAP(vorname) AS "FirstName",
    INITCAP(nachname) AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN lower(funktion) = 'decision maker' THEN 'Decision Maker'
        WHEN lower(funktion) = 'end user' THEN 'End User'
        WHEN lower(funktion) = 'technical contact' THEN 'Technical Contact'
        WHEN lower(funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN lower(sprache) = 'de' THEN 'DE'
        WHEN lower(sprache) = 'en' THEN 'EN'
        WHEN lower(sprache) = 'fr' THEN 'FR'
        WHEN lower(sprache) = 'es' THEN 'ES'
        WHEN lower(sprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
