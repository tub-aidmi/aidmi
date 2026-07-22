{{ config(materialized='table') }}

SELECT
    CAST(ap.ap_id AS TEXT) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(ap.nachname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENTSCHEIDER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) IN ('ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) IN ('TECHNIKER', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) IN ('VORSTAND', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap.kunde) = k.kunden_nr