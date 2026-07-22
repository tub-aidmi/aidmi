{{ config(materialized='table') }}
WITH source_data AS (
  SELECT
    k.kontakt_id,
    k.rufname,
    k.familienname,
    k.kontakt_email,
    k.tel,
    k.berufsbezeichnung,
    k.rolle,
    k.korrespondenzsprache,
    k.kd_nummer
  FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
)
SELECT
  gen_random_uuid()::text AS "Id",
  INITCAP(TRIM(rufname)) AS "FirstName",
  INITCAP(TRIM(familienname)) AS "LastName",
  LOWER(TRIM(kontakt_email)) AS "Email",
  TRIM(tel) AS "Phone",
  INITCAP(TRIM(berufsbezeichnung)) AS "Title",
  CASE
    WHEN UPPER(TRIM(rolle)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(rolle)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
    WHEN UPPER(TRIM(rolle)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
    WHEN UPPER(TRIM(rolle)) IN ('EXECUTIVE SPONSOR', 'GESCHÄFTSFÜHRER') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH') THEN 'EN'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
    WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  CASE WHEN TRIM(kd_nummer) IS NOT NULL THEN md5('ns:' || TRIM(kd_nummer)) ELSE NULL END AS "AccountId",
  TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM source_data