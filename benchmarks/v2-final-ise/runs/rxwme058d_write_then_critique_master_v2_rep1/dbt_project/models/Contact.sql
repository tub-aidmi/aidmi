{{ config(materialized='table') }}
SELECT
  MD5(k.kontakt_id) AS "Id",
  TRIM(k.rufname) AS "FirstName",
  TRIM(k.familienname) AS "LastName",
  CASE WHEN k.kontakt_email IS NULL OR k.kontakt_email = '' THEN NULL ELSE LOWER(TRIM(k.kontakt_email)) END AS "Email",
  CASE WHEN k.tel IS NULL OR k.tel = '' THEN NULL ELSE TRIM(k.tel) END AS "Phone",
  CASE WHEN k.berufsbezeichnung IS NULL OR k.berufsbezeichnung = '' THEN NULL ELSE INITCAP(LOWER(TRIM(k.berufsbezeichnung))) END AS "Title",
  CASE 
    WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
    WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
    WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL 
  END AS "Role__c",
  CASE 
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ENGLISH', 'ENGLISCH') THEN 'EN'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FRENCH', 'FRANÇAIS') THEN 'FR'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('SPANISH', 'ESPAÑOL') THEN 'ES'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ITALIAN', 'ITALIENISCH') THEN 'IT'
    ELSE NULL 
  END AS "Preferred_Language__c",
  CASE WHEN c.kundennummer IS NOT NULL THEN MD5(c.kundennummer) ELSE NULL END AS "AccountId",
  k.kontakt_id AS "Legacy_Contact_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c ON TRIM(k.kd_nummer) = TRIM(c.kundennummer)