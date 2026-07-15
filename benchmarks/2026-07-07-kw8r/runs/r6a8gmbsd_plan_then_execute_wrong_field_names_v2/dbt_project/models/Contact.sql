{{ config(materialized='table') }}

WITH contact_data AS (
  SELECT
    gen_random_uuid() AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(TRIM(ap.nachname)) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
      WHEN TRIM(ap.funktion) IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') 
      THEN TRIM(ap.funktion)
      ELSE NULL
    END AS "Role__c",
    CASE 
      WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
      THEN UPPER(TRIM(ap.sprache))
      ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
  LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON ap.kunde = k.kunden_nr
)

SELECT
  "Id",
  "FirstName",
  "LastName",
  "Email",
  "Phone",
  "Title",
  "Role__c",
  "Preferred_Language__c",
  "AccountId",
  "Legacy_Contact_ID__c",
  "CreatedDate",
  "LastModifiedDate",
  "IsDeleted"
FROM contact_data