{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(TRIM(ap.nachname)) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN UPPER(TRIM(ap.funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) = 'END USER' THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
