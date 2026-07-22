{{ config(materialized='table') }}
SELECT 
    ap.ap_id AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), '') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE 
        WHEN TRIM(LOWER(ap.funktion)) IN ('entscheider') THEN 'Decision Maker'
        WHEN TRIM(LOWER(ap.funktion)) IN ('anwender') THEN 'End User'
        WHEN TRIM(LOWER(ap.funktion)) IN ('technischer kontakt', 'technischerkontakt') THEN 'Technical Contact'
        WHEN TRIM(LOWER(ap.funktion)) IN ('führungssponsor', 'fuhrungssponsor') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(ap.sprache)) IN ('deutsch') THEN 'DE'
        WHEN TRIM(LOWER(ap.sprache)) IN ('englisch') THEN 'EN'
        WHEN TRIM(LOWER(ap.sprache)) IN ('französisch', 'franzosisch') THEN 'FR'
        WHEN TRIM(LOWER(ap.sprache)) IN ('spanisch') THEN 'ES'
        WHEN TRIM(LOWER(ap.sprache)) IN ('italienisch') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)