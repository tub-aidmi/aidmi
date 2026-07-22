{{ config(materialized='table') }}

WITH contact_data AS (
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
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '003' || SUBSTRING(MD5(cd.ap_id), 1, 15) AS "Id",
    cd.vorname AS "FirstName",
    cd.nachname AS "LastName",
    cd.email_adresse AS "Email",
    cd.telefonnummer AS "Phone",
    cd.position AS "Title",
    CASE 
        WHEN UPPER(TRIM(cd.funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(cd.funktion)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(cd.funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(cd.funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(cd.sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(cd.sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(cd.sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(cd.sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(cd.sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ai.account_id AS "AccountId",
    cd.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data cd
LEFT JOIN account_ids ai ON cd.kunde = ai.kunden_nr
