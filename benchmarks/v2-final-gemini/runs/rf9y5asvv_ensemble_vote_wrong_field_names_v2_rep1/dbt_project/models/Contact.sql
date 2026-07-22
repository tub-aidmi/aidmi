{{ config(materialized='table') }}

WITH source_contacts AS (
    SELECT
        ap_id,
        vorname,
        nachname,
        email_adresse,
        telefonnummer,
        position,
        funktion,
        sprache,
        kunde
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
)
SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN LOWER(funktion) = 'entscheidungsträger' THEN 'Decision Maker'
        WHEN LOWER(funktion) = 'endnutzer' THEN 'End User'
        WHEN LOWER(funktion) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN LOWER(funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(sprache) = 'deutsch' THEN 'DE'
        WHEN LOWER(sprache) = 'englisch' THEN 'EN'
        WHEN LOWER(sprache) = 'französisch' THEN 'FR'
        WHEN LOWER(sprache) = 'spanisch'
             OR LOWER(sprache) = 'spanisch' THEN 'ES'
        WHEN LOWER(sprache) = 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId", -- AccountId is directly mapped from the customer ID for simplicity as per Account model
    ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_contacts
