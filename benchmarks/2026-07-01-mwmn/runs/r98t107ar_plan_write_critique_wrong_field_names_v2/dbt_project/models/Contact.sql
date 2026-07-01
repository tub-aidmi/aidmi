{{ config(materialized='table') }}
WITH account_mapping AS (
  SELECT
    TRIM(kunden_nr) AS kunden_nr,
    LEFT(MD5(TRIM(kunden_nr)), 18) AS account_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
  LEFT(MD5(TRIM(ap.ap_id)), 18) AS "Id",
  TRIM(INITCAP(ap.vorname)) AS "FirstName",
  COALESCE(TRIM(INITCAP(ap.nachname)), 'Unknown') AS "LastName",
  TRIM(ap.email_adresse) AS "Email",
  TRIM(ap.telefonnummer) AS "Phone",
  TRIM(INITCAP(ap.position)) AS "Title",
  CASE
    WHEN TRIM(UPPER(ap.funktion)) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    WHEN TRIM(UPPER(ap.funktion)) IN ('TECHNICAL CONTACT') THEN 'Technical Contact'
    WHEN TRIM(UPPER(ap.funktion)) IN ('DECISION MAKER') THEN 'Decision Maker'
    WHEN TRIM(UPPER(ap.funktion)) IN ('END USER') THEN 'End User'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN TRIM(UPPER(ap.sprache)) IN ('DE', 'GERMAN') THEN 'DE'
    WHEN TRIM(UPPER(ap.sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
    WHEN TRIM(UPPER(ap.sprache)) IN ('FR', 'FRENCH') THEN 'FR'
    WHEN TRIM(UPPER(ap.sprache)) IN ('ES', 'SPANISH') THEN 'ES'
    WHEN TRIM(UPPER(ap.sprache)) IN ('IT', 'ITALIAN') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  am.account_id AS "AccountId",
  TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN account_mapping am ON TRIM(ap.kunde) = am.kunden_nr