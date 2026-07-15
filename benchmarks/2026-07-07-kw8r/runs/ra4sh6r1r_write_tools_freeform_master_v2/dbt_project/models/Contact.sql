{{ config(materialized='table') }}

WITH contact_data AS (
  SELECT 
    '' || MD5(COALESCE(kontakt_id, '') || COALESCE(rufname, '') || COALESCE(familienname, '')) AS contact_id,
    
    -- Join to customer to get AccountId
    '' || MD5(COALESCE(kd.kundennummer, '') || COALESCE(kd.unternehmensname, '')) AS account_id,
    
    TRIM(INITCAP(COALESCE(NULLIF(rufname, ''), 'Unknown'))) AS first_name,
    TRIM(INITCAP(COALESCE(NULLIF(familienname, ''), 'Unknown'))) AS last_name,
    LOWER(TRIM(COALESCE(NULLIF(kontakt_email, ''), NULL))) AS email,
    TRIM(tel) AS phone_raw,
    TRIM(berufsbezeichnung) AS title,
    
    -- Map rolle to Role__c
    CASE 
      WHEN UPPER(TRIM(rolle)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER', 'DECISION MAKER') THEN 'Decision Maker'
      WHEN UPPER(TRIM(rolle)) IN ('ENDNUTZER', 'END USER', 'END USER') THEN 'End User'
      WHEN UPPER(TRIM(rolle)) IN ('TECHNISCHER KONTAKT', 'TECHNISCH', 'TECHNICAL CONTACT') THEN 'Technical Contact'
      WHEN UPPER(TRIM(rolle)) IN ('EXECUTIVE SPONSOR', 'GESCHÄFTSFÜHRER', 'GESCHÄFTSFÜHRUNG') THEN 'Executive Sponsor'
      ELSE NULL
    END AS role,
    
    -- Map korrespondenzsprache to Preferred_Language__c
    CASE 
      WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DEUTSCH', 'DE', 'GERMAN') THEN 'DE'
      WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ENGLISCH', 'EN', 'ENGLISH') THEN 'EN'
      WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FRANZÖSISCH', 'FR', 'FRENCH') THEN 'FR'
      WHEN UPPER(TRIM(korrespondenzsprache)) IN ('SPANISCH', 'ES', 'SPANISH') THEN 'ES'
      WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ITALIENISCH', 'IT', 'ITALIAN') THEN 'IT'
      ELSE NULL
    END AS preferred_language,
    
    TRIM(kontakt_id) AS legacy_contact_id,
    '2024-01-01' AS created_date,
    '2024-01-01' AS last_modified_date,
    0 AS is_deleted
    
  FROM {{ source(source_slug, 'master_kontakte') }} k
  LEFT JOIN {{ source(source_slug, 'master_kunden') }} kd 
    ON TRIM(k.kd_nummer) = TRIM(kd.kundennummer)
)

SELECT 
  contact_id AS "Id",
  first_name AS "FirstName",
  last_name AS "LastName",
  email AS "Email",
  
  -- Handle phone: use tel if it looks like a phone number
  CASE 
    WHEN phone_raw ~ '^[0-9+\-\s\(\)]+$' THEN TRIM(phone_raw)
    ELSE NULL 
  END AS "Phone",
  
  title AS "Title",
  role AS "Role__c",
  preferred_language AS "Preferred_Language__c",
  account_id AS "AccountId",
  legacy_contact_id AS "Legacy_Contact_ID__c",
  created_date AS "CreatedDate",
  last_modified_date AS "LastModifiedDate",
  is_deleted AS "IsDeleted"

FROM contact_data
