{{ config(materialized='table') }}
WITH customer_accounts AS (
  SELECT kundennummer, MD5(kundennummer) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)
SELECT
  MD5(TRIM(k.kontakt_id)) AS "Id",
  INITCAP(TRIM(k.rufname)) AS "FirstName",
  INITCAP(TRIM(k.familienname)) AS "LastName",
  LOWER(TRIM(k.kontakt_email)) AS "Email",
  TRIM(k.tel) AS "Phone",
  INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
  CASE
    WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
    WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
    WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ENGLISH', 'ENGLISCH', 'EN') THEN 'EN'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FRANZÖSISCH', 'FRENCH', 'FR') THEN 'FR'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('SPANISCH', 'SPANISH', 'ES') THEN 'ES'
    WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ITALIENISCH', 'ITALIAN', 'IT') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  ca.account_id AS "AccountId",
  TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
  '2023-01-01T00:00:00Z' AS "CreatedDate",
  '2023-01-01T00:00:00Z' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN customer_accounts ca ON TRIM(k.kd_nummer) = ca.kundennummer
WHERE k.kd_nummer IS NULL OR k.kd_nummer LIKE 'CUST-%'