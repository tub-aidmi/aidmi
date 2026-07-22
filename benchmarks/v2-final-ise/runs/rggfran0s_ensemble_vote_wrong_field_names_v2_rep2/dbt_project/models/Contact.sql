{{ config(materialized='table') }}

WITH account_key_map AS (
  SELECT
    kunden_nr,
    -- Salesforce Account Id: prefix '001' + zero-padded key to 15 chars = 18-char Id with checksum omitted for simplicity in staging
    LEFT('001' || LPAD(kunden_nr, 32, '0'), 18) AS "account_id_18"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
  -- Contact Id: derive from ap_id using Salesforce Contact prefix '003'
  LEFT('003' || LPAD(ap_id, 32, '0'), 18) AS "Id",

  -- First Name (Vorname in German source)
  INITCAP(TRIM(vorname)) AS "FirstName",

  -- Last Name (Nachname in German source) - NOT NULL: use empty string fallback only if truly required
  COALESCE(INITCAP(TRIM(nachname)), '') AS "LastName",

  -- Email
  LOWER(TRIM(email_adresse)) AS "Email",

  -- Phone (Telefonnummer)
  TRIM(telefonnummer) AS "Phone",

  -- Title / Position
  INITCAP(TRIM(position)) AS "Title",

  -- Role__c: map funktion enum values
  CASE
    WHEN LOWER(TRIM(funktion)) IN ('entscheider', 'decision maker', 'ent-decision_maker') THEN 'Decision Maker'
    WHEN LOWER(TRIM(funktion)) IN ('nutzer', 'end user', 'end-user', 'nutzer_end_user') THEN 'End User'
    WHEN LOWER(TRIM(funktion)) IN ('technisch', 'technical contact', 'tech-contact', 'technischer_kontakt') THEN 'Technical Contact'
    WHEN LOWER(TRIM(funktion)) IN ('fuenforderer', 'executive sponsor', 'sponsor', 'ausbilder_sponsor') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",

  -- Preferred_Language__c: map sprache to ISO codes
  CASE
    WHEN LOWER(TRIM(sprache)) IN ('de', 'german', 'deutsch') THEN 'DE'
    WHEN LOWER(TRIM(sprache)) IN ('en', 'english', 'eng') THEN 'EN'
    WHEN LOWER(TRIM(sprache)) IN ('fr', 'french', 'fran') THEN 'FR'
    WHEN LOWER(TRIM(sprache)) IN ('es', 'spanish', 'esp') THEN 'ES'
    WHEN LOWER(TRIM(sprache)) IN ('it', 'italian', 'ital') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",

  -- AccountId: reference Salesforce Account Id via kunden join
  ak.account_id_18 AS "AccountId",

  -- Legacy Contact ID from source primary key
  ap_id AS "Legacy_Contact_ID__c",

  -- Dates: no date columns in ansprechpartner source; default to NULL per guidelines
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",

  -- IsDeleted: not present in source; default to 0 (not deleted)
  0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
JOIN account_key_map ak ON a.kunde = ak.kunden_nr
WHERE TRIM(a.ap_id) != ''