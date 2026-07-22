{{ config(materialized='table') }}

WITH account_ids AS (
    SELECT 
        TRIM(kunden_nr) AS kunden_nr,
        '001' || LPAD(REGEXP_REPLACE(TRIM(kunden_nr), '^[A-Za-z-]+', ''), 15, '0') AS sf_account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT 
    '002' || LPAD(REGEXP_REPLACE(TRIM(ap.ap_id), '^[A-Za-z-]+', ''), 15, '0') AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(ap.nachname)), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(ap.funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(ap.funktion)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(ap.funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(ap.funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRENCH', 'FRANCAIS') THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISH', 'ESPANOL') THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIAN', 'ITALIANO') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    a.sf_account_id AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
JOIN account_ids a 
    ON TRIM(ap.kunde) = a.kunden_nr