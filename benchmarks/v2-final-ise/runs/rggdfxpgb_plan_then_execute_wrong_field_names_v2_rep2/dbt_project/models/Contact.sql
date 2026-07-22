{{ config(materialized='table') }}

WITH account_refs AS (
  SELECT 
    TRIM(kunden_nr) AS src_kunde,
    'a00' || TRIM(kunden_nr) AS sf_account_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
  WHERE TRIM(kunden_nr) IS NOT NULL AND TRIM(kunden_nr) != ''
)

SELECT 
  'a03' || TRIM(ap.ap_id) AS "Id",
  TRIM(ap.vorname) AS "FirstName",
  COALESCE(INITCAP(TRIM(ap.nachname)), 'Unknown Contact') AS "LastName",
  LOWER(TRIM(ap.email_adresse)) AS "Email",
  TRIM(ap.telefonnummer) AS "Phone",
  INITCAP(TRIM(ap.position)) AS "Title",
  CASE 
    WHEN LOWER(TRIM(ap.funktion)) IN ('decision maker', 'entscheider', 'decision-maker') THEN 'Decision Maker'
    WHEN LOWER(TRIM(ap.funktion)) IN ('end user', 'nutzer', 'end-user') THEN 'End User'
    WHEN LOWER(TRIM(ap.funktion)) IN ('technical contact', 'technischer kontakt', 'tech contact') THEN 'Technical Contact'
    WHEN LOWER(TRIM(ap.funktion)) IN ('executive sponsor', 'vorstandssponsor', 'exec sponsor') THEN 'Executive Sponsor'
    ELSE NULL 
  END AS "Role__c",
  CASE 
    WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'GERMAN', 'DEUTSCH') THEN 'DE'
    WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
    WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
    WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
    WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
    ELSE NULL 
  END AS "Preferred_Language__c",
  ar.sf_account_id AS "AccountId",
  TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN account_refs ar 
  ON NULLIF(TRIM(ap.kunde), '') = ar.src_kunde