{{ config(materialized='table') }}

SELECT 
    MD5(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    TRIM(LOWER(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE 
        WHEN TRIM(LOWER(ap.funktion)) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(ap.funktion)) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN TRIM(LOWER(ap.funktion)) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(ap.funktion)) IN ('exekutiver sponsor', 'executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(ap.sprache)) IN ('deutsch', 'de', 'ger') THEN 'DE'
        WHEN TRIM(LOWER(ap.sprache)) IN ('englisch', 'en', 'eng') THEN 'EN'
        WHEN TRIM(LOWER(ap.sprache)) IN ('französisch', 'fr', 'fra') THEN 'FR'
        WHEN TRIM(LOWER(ap.sprache)) IN ('spanisch', 'es', 'spa') THEN 'ES'
        WHEN TRIM(LOWER(ap.sprache)) IN ('italienisch', 'it', 'ita') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)