{{ config(materialized='table') }}

SELECT
    CAST(TRIM(ap.ap_id) AS TEXT) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(ap.nachname)), 'Unknown') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENTSCHEIDER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENDBENUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) IN ('EXECUTIVE SPONSOR', 'VORSITZENDER SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) LIKE '%DE%' OR TRIM(ap.sprache) = 'Deutsch' THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) LIKE '%EN%' OR TRIM(ap.sprache) = 'English' THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) LIKE '%FR%' OR TRIM(ap.sprache) = 'Französisch' THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) LIKE '%ES%' OR TRIM(ap.sprache) = 'Spanisch' THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) LIKE '%IT%' OR TRIM(ap.sprache) = 'Italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE 
        WHEN TRIM(k.kunden_nr) LIKE 'K%' THEN '001' || SUBSTRING(TRIM(k.kunden_nr) FROM 2)
        ELSE NULL
    END AS "AccountId",
    CAST(TRIM(ap.ap_id) AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)