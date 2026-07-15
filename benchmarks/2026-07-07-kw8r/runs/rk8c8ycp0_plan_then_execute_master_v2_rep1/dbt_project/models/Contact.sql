{{ config(materialized='table') }}

SELECT
  INITCAP(TRIM(mk.kontakt_id)) AS "Id",
  TRIM(mk.rufname) AS "FirstName",
  COALESCE(INITCAP(TRIM(mk.familienname)), 'Unknown') AS "LastName",
  mk.kontakt_email AS "Email",
  TRIM(mk.tel) AS "Phone",
  mk.berufsbezeichnung AS "Title",
  CASE LOWER(TRIM(mk.rolle))
    WHEN 'decision maker' THEN 'Decision Maker'
    WHEN 'end user' THEN 'End User'
    WHEN 'technical contact' THEN 'Technical Contact'
    WHEN 'executive sponsor' THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE UPPER(TRIM(mk.korrespondenzsprache))
    WHEN 'DE' THEN 'DE'
    WHEN 'EN' THEN 'EN'
    WHEN 'FR' THEN 'FR'
    WHEN 'ES' THEN 'ES'
    WHEN 'IT' THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  INITCAP(TRIM(mkun.kundennummer)) AS "AccountId",
  mk.kontakt_id AS "Legacy_Contact_ID__c",
  '1900-01-01' AS "CreatedDate",
  '1900-01-01' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mkun
  ON INITCAP(TRIM(mk.kd_nummer)) = INITCAP(TRIM(mkun.kundennummer))