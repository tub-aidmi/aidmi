{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        ap.ap_id,
        ap.vorname,
        ap.nachname,
        ap.email_adresse,
        ap.telefonnummer,
        ap.position,
        ap.funktion,
        ap.sprache,
        ap.kunde,
        k.kunden_nr AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
)

SELECT
    ap_id AS "Id",
    TRIM(vorname) AS "FirstName",
    TRIM(nachname) AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE 
        WHEN TRIM(UPPER(funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN TRIM(UPPER(funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN TRIM(UPPER(funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN TRIM(UPPER(funktion)) = 'END USER' THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(UPPER(sprache)) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(sprache)) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(sprache)) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(sprache)) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    account_id AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_source