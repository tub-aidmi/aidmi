{{ config(materialized='table') }}

SELECT
  UPPER(TRIM(k.kontakt_id)) AS "Id",
  INITCAP(TRIM(k.rufname)) AS "FirstName",
  COALESCE(INITCAP(TRIM(k.familienname)), 'Unknown') AS "LastName",
  LOWER(TRIM(k.kontakt_email)) AS "Email",
  TRIM(k.tel) AS "Phone",
  INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
  CASE UPPER(TRIM(k.rolle))
    WHEN 'DECISION_MAKER' THEN 'Decision Maker'
    WHEN 'END_USER'   THEN 'End User'
    WHEN 'TECH_CONTACT' THEN 'Technical Contact'
    WHEN 'EXEC_SPONSOR' THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE UPPER(TRIM(k.korrespondenzsprache))
    WHEN 'DE' THEN 'DE'
    WHEN 'EN' THEN 'EN'
    WHEN 'FR' THEN 'FR'
    WHEN 'ES' THEN 'ES'
    WHEN 'IT' THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  UPPER(TRIM(a.kundennummer)) AS "AccountId",
  k.kontakt_id AS "Legacy_Contact_ID__c",
  '2024-01-01 00:00:00' AS "CreatedDate",
  '2024-01-01 00:00:00' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} a
  ON UPPER(TRIM(k.kd_nummer)) = UPPER(TRIM(a.kundennummer))