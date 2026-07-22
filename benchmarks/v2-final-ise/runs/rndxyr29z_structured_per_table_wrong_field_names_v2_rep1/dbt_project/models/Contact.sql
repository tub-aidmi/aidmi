{{ config(materialized='table') }}

SELECT
    '003' || REGEXP_REPLACE(ap.ap_id, '[^0-9A-Za-z]', '', 'g') AS "Id",
    CASE WHEN TRIM(vorname) = '' THEN NULL ELSE INITCAP(TRIM(vorname)) END AS "FirstName",
    CASE WHEN TRIM(nachname) = '' THEN 'Unknown' ELSE UPPER(TRIM(nachname)) END AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(funktion)) IN ('decision maker', 'dm', 'entscheider', 'decision-maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(funktion)) IN ('end user', 'eu', 'endanwender', 'end-user') THEN 'End User'
        WHEN LOWER(TRIM(funktion)) IN ('technical contact', 'tc', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(funktion)) IN ('executive sponsor', 'es', 'führungskraft', 'executive') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'GERMAN', 'DEU') THEN 'DE'
        WHEN UPPER(TRIM(sprache)) IN ('EN', 'ENGLISH', 'ENG') THEN 'EN'
        WHEN UPPER(TRIM(sprache)) IN ('FR', 'FRENCH', 'FRE') THEN 'FR'
        WHEN UPPER(TRIM(sprache)) IN ('ES', 'SPANISH', 'SPA') THEN 'ES'
        WHEN UPPER(TRIM(sprache)) IN ('IT', 'ITALIAN', 'ITA') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || REGEXP_REPLACE(ak.kunden_nr, '[^0-9A-Za-z]', '', 'g') AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} ak 
    ON TRIM(ap.kunde) = TRIM(ak.kunden_nr)