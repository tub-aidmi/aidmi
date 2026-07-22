{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNISCHER CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) = 'DEUTSCH' THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) = 'ENGLISCH' THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) = 'FRANZÖSISCH' THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) = 'SPANISCH' THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) = 'ITALIENISCH' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
