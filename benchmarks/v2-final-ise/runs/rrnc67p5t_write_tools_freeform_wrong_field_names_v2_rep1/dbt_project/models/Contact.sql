{{ config(materialized='table') }}

SELECT
    CAST('003' || ap.ap_id AS TEXT) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(TRIM(ap.nachname)) AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    ap.telefonnummer AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST('001' || k.kunden_nr AS TEXT) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON ap.kunde = k.kunden_nr
