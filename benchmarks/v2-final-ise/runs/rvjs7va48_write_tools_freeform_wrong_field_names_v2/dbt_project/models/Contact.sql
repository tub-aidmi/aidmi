{{ config(materialized='table') }}

WITH contact_data AS (
  SELECT
    ap.ap_id AS legacy_contact_id,
    ap.vorname,
    ap.nachname,
    ap.email_adresse,
    ap.telefonnummer,
    ap.position,
    ap.funktion,
    ap.sprache,
    ap.kunde AS customer_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
)

SELECT
  legacy_contact_id AS "Id",
  INITCAP(TRIM(vorname)) AS "FirstName",
  INITCAP(TRIM(nachname)) AS "LastName",
  LOWER(TRIM(email_adresse)) AS "Email",
  TRIM(telefonnummer) AS "Phone",
  INITCAP(TRIM(position)) AS "Title",
  CASE 
    WHEN UPPER(TRIM(funktion)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(funktion)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
    WHEN UPPER(TRIM(funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
    WHEN UPPER(TRIM(funktion)) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE 
    WHEN UPPER(TRIM(sprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
    WHEN UPPER(TRIM(sprache)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
    WHEN UPPER(TRIM(sprache)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
    WHEN UPPER(TRIM(sprache)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  customer_id AS "AccountId",
  legacy_contact_id AS "Legacy_Contact_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM contact_data