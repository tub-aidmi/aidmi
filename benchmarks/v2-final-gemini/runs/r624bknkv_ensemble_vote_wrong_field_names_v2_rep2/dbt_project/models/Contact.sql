-- noinspection SqlNoDataSourceInspectionForFile
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
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
)
SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, '') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) LIKE '%endnutzer%' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) LIKE '%technisch%' OR LOWER(TRIM(ap.funktion)) LIKE '%techniker%' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) LIKE '%projektleiter%' OR LOWER(TRIM(ap.funktion)) LIKE '%management%' OR LOWER(TRIM(ap.funktion)) LIKE '%leitung%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(ap.sprache)) = 'deutsch' THEN 'DE'
        WHEN LOWER(TRIM(ap.sprache)) = 'englisch' THEN 'EN'
        WHEN LOWER(TRIM(ap.sprache)) = 'französisch' THEN 'FR'
        WHEN LOWER(TRIM(ap.sprache)) = 'spanisch' THEN 'ES'
        WHEN LOWER(TRIM(ap.sprache)) = 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_contacts ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
ON
    ap.kunde = k.kunden_nr;