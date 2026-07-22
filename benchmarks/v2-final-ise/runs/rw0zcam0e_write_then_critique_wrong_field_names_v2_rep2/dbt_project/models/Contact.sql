{{ config(materialized='table') }}
SELECT 
    '001' || ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    ap.nachname AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE 
        WHEN LOWER(TRIM(ap.funktion)) IN ('decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) IN ('end user') THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) IN ('technical contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || REGEXP_REPLACE(k.kunden_nr, '^CUST-', '') AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON REGEXP_REPLACE(ap.kunde, '^CUST-', '') = REGEXP_REPLACE(k.kunden_nr, '^CUST-', '')