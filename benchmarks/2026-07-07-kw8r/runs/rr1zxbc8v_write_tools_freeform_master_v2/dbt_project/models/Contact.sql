{{ config(materialized='table') }}

WITH contact_data AS (
  SELECT
    kontakt_id,
    rufname,
    familienname,
    kontakt_email,
    tel,
    berufsbezeichnung,
    rolle,
    korrespondenzsprache,
    kd_nummer
  FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
),

account_mapping AS (
  SELECT
    kundennummer AS legacy_customer_id,
    SUBSTRING(MD5('Account_' || kundennummer) FROM 1 FOR 18) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
  -- Generate deterministic Salesforce-style Id
  SUBSTRING(MD5('Contact_' || c.kontakt_id) FROM 1 FOR 18) AS "Id",
  
  -- FirstName
  NULLIF(TRIM(c.rufname), '') AS "FirstName",
  
  -- LastName: required, use familienname or kontakt_id as fallback
  COALESCE(NULLIF(TRIM(c.familienname), ''), c.kontakt_id) AS "LastName",
  
  -- Email
  NULLIF(TRIM(c.kontakt_email), '') AS "Email",
  
  -- Phone
  NULLIF(TRIM(c.tel), '') AS "Phone",
  
  -- Title
  NULLIF(TRIM(c.berufsbezeichnung), '') AS "Title",
  
  -- Role: normalize to enum values
  CASE 
    WHEN UPPER(TRIM(c.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(c.rolle)) IN ('END USER', 'ENDANWENDER', 'END USER') THEN 'End User'
    WHEN UPPER(TRIM(c.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNIKER') THEN 'Technical Contact'
    WHEN UPPER(TRIM(c.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  
  -- Preferred Language: normalize to 2-letter codes
  CASE 
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('ENGLISH', 'EN', 'ENGLISCH') THEN 'EN'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('FRANZÖSISCH', 'FR', 'FRENCH') THEN 'FR'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('ESPAÑOL', 'ES', 'SPANISH') THEN 'ES'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('ITALIANO', 'IT', 'ITALIENISCH') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  
  -- AccountId: lookup from master_kunden via kd_nummer
  am.account_id AS "AccountId",
  
  -- Legacy Contact ID
  c.kontakt_id AS "Legacy_Contact_ID__c",
  
  -- CreatedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  
  -- LastModifiedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- IsDeleted: default to 0
  0 AS "IsDeleted"

FROM contact_data c
LEFT JOIN account_mapping am ON c.kd_nummer = am.legacy_customer_id
