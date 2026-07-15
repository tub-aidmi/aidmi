{{ config(materialized='table') }}

SELECT
  UPPER(TRIM(kontakt_id)) AS "Id",
  COALESCE(NULLIF(TRIM(rufname), ''), 'N/A') AS "FirstName",
  COALESCE(NULLIF(TRIM(familienname), ''), 'Unknown') AS "LastName",
  LOWER(TRIM(kontakt_email)) AS "Email",
  REGEXP_REPLACE(COALESCE(NULLIF(TRIM(tel), ''), ''), '[^0-9+]', '', 'g') AS "Phone",
  INITCAP(TRIM(berufsbezeichnung)) AS "Title",
  CASE
    WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'dm') THEN 'Decision Maker'
    WHEN LOWER(TRIM(rolle)) IN ('end user', 'eu')   THEN 'End User'
    WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'tc') THEN 'Technical Contact'
    WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'es') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DEUTSCH', 'GERMAN', 'DE')     THEN 'DE'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ENGLISH', 'ENG', 'EN')        THEN 'EN'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FRENCH', 'FRANCAIS', 'FR')    THEN 'FR'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('SPANISH', 'ESPANOL', 'ES')    THEN 'ES'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ITALIAN', 'ITALIANO', 'IT')   THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  UPPER(TRIM(REGEXP_REPLACE(UPPER(TRIM(kd_nummer)), '^(KUN|CUST|KD)-', ''))) AS "AccountId",
  kontakt_id AS "Legacy_Contact_ID__c",
  '2024-01-01 00:00:00'        AS "CreatedDate",
  '2024-01-01 00:00:00'        AS "LastModifiedDate",
  0                            AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}