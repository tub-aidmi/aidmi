{{ config(materialized='table') }}

WITH ansprechpartner_data AS (
  SELECT
    ap_id,
    vorname,
    nachname,
    email_adresse,
    telefonnummer,
    position,
    funktion,
    sprache,
    kunde
  FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
),

account_mapping AS (
  SELECT
    kunden_nr AS "AccountId",
    kunden_nr AS "Legacy_Customer_ID__c"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
  ap_id AS "Id",
  INITCAP(TRIM(vorname)) AS "FirstName",
  INITCAP(TRIM(nachname)) AS "LastName",
  LOWER(TRIM(email_adresse)) AS "Email",
  telefonnummer AS "Phone",
  INITCAP(TRIM(position)) AS "Title",
  CASE
    WHEN UPPER(TRIM(funktion)) IN ('ENTSCHEIDER', 'ENTSCHEIDUNGSTRÄGER', 'DECISION MAKER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(funktion)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
    WHEN UPPER(TRIM(funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
    WHEN UPPER(TRIM(funktion)) IN ('GESCHÄFTSFÜHRER', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(TRIM(sprache)) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(sprache)) IN ('ENGLISCH', 'EN', 'ENGLISH') THEN 'EN'
    WHEN UPPER(TRIM(sprache)) IN ('FRANZÖSISCH', 'FR', 'FRENCH') THEN 'FR'
    WHEN UPPER(TRIM(sprache)) IN ('SPANISCH', 'ES', 'SPANISH') THEN 'ES'
    WHEN UPPER(TRIM(sprache)) IN ('ITALIENISCH', 'IT', 'ITALIAN') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  account_mapping."AccountId" AS "AccountId",
  ap_id AS "Legacy_Contact_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM ansprechpartner_data
LEFT JOIN account_mapping ON ansprechpartner_data.kunde = account_mapping."Legacy_Customer_ID__c"
