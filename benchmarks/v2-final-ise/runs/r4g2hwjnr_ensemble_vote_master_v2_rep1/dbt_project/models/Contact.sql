{{ config(materialized='table') }}

WITH account_map AS (
  SELECT
    '001' || kundennummer AS sf_account_id,
    kundennummer
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
contacts_raw AS (
  SELECT
    mk.kontakt_id,
    mk.rufname,
    mk.familienname,
    mk.kontakt_email,
    mk.tel,
    mk.berufsbezeichnung,
    mk.rolle,
    mk.korrespondenzsprache,
    am.sf_account_id AS account_id_ref
  FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
  LEFT JOIN account_map am
    ON TRIM(mk.kd_nummer) = TRIM(am.kundennummer)
)
SELECT
  '003' || kontakt_id AS "Id",
  INITCAP(TRIM(rufname)) AS "FirstName",
  COALESCE(NULLIF(TRIM(familienname), ''), '') AS "LastName",
  LOWER(TRIM(kontakt_email)) AS "Email",
  TRIM(tel) AS "Phone",
  INITCAP(TRIM(berufsbezeichnung)) AS "Title",
  CASE
    WHEN UPPER(TRIM(rolle)) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
    WHEN UPPER(TRIM(rolle)) LIKE '%ENDBENUTZER%' THEN 'End User'
    WHEN UPPER(TRIM(rolle)) LIKE '%TECHNISCH%' THEN 'Technical Contact'
    WHEN UPPER(TRIM(rolle)) LIKE '%EXECUTIVE SPONSOR%' OR UPPER(TRIM(rolle)) LIKE '%VORSITZ%' THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'GERMAN', 'DEU') THEN 'DE'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENG') THEN 'EN'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRE', 'FRA') THEN 'FR'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPA') THEN 'ES'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITA') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  account_id_ref AS "AccountId",
  kontakt_id AS "Legacy_Contact_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM contacts_raw