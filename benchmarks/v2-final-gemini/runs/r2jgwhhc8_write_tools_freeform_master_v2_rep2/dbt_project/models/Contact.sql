{{
  config(
    materialized='table'
  )
}}

SELECT
  MD5(master_kontakte.kontakt_id) AS "Id",
  master_kontakte.rufname AS "FirstName",
  COALESCE(master_kontakte.familienname, 'Unknown') AS "LastName",
  master_kontakte.kontakt_email AS "Email",
  master_kontakte.tel AS "Phone",
  master_kontakte.berufsbezeichnung AS "Title",
  CASE
    WHEN LOWER(master_kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
    WHEN LOWER(master_kontakte.rolle) = 'end user' THEN 'End User'
    WHEN LOWER(master_kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
    WHEN LOWER(master_kontakte.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  CASE
    WHEN UPPER(master_kontakte.korrespondenzsprache) = 'DE' THEN 'DE'
    WHEN UPPER(master_kontakte.korrespondenzsprache) = 'EN' THEN 'EN'
    WHEN UPPER(master_kontakte.korrespondenzsprache) = 'FR' THEN 'FR'
    WHEN UPPER(master_kontakte.korrespondenzsprache) = 'ES' THEN 'ES'
    WHEN UPPER(master_kontakte.korrespondenzsprache) = 'IT' THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  MD5(master_kontakte.kd_nummer) AS "AccountId", -- Assuming kd_nummer links to master_kunden.kundennummer
  master_kontakte.kontakt_id AS "Legacy_Contact_ID__c",
  NOW()::TEXT AS "CreatedDate",
  NOW()::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_kontakte') }} AS master_kontakte
