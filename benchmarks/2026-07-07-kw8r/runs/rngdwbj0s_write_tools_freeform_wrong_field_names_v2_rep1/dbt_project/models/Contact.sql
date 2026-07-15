{{ config(materialized='table') }}

SELECT
    'CONT_' || ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    ap.nachname AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) IN ('EXECUTIVE SPONSOR', 'GESCHÄFTSFÜHRER') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRANZÖSISCH', 'FRANZOSISCH') THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'ACCT_' || k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
