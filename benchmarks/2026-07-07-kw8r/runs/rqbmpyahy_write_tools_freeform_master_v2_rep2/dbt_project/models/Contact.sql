{{ config(materialized='table') }}

WITH contact_data AS (
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
),

account_mapping AS (
  SELECT 
    kundennummer,
    '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
  -- Generate deterministic Salesforce-style ID from natural key
  '003' || SUBSTRING(MD5(c.kontakt_id) FROM 1 FOR 15) AS "Id",
  
  -- First Name
  NULLIF(TRIM(c.rufname), '') AS "FirstName",
  
  -- Last Name (required)
  COALESCE(NULLIF(TRIM(c.familienname), ''), 'Unknown') AS "LastName",
  
  -- Email
  CASE 
    WHEN c.kontakt_email ~ '^[^@]+@[^@]+\.[^@]+$' THEN LOWER(TRIM(c.kontakt_email))
    ELSE NULL
  END AS "Email",
  
  -- Phone
  NULLIF(REGEXP_REPLACE(TRIM(c.tel), '[^0-9+]', '', 'g'), '') AS "Phone",
  
  -- Title
  NULLIF(TRIM(c.berufsbezeichnung), '') AS "Title",
  
  -- Role: map to enum values
  CASE 
    WHEN UPPER(TRIM(c.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
    WHEN UPPER(TRIM(c.rolle)) IN ('END USER', 'ENDANWENDER', 'END USER', 'ENDANWENDER') THEN 'End User'
    WHEN UPPER(TRIM(c.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
    WHEN UPPER(TRIM(c.rolle)) IN ('EXECUTIVE SPONSOR', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
    ELSE NULL
  END AS "Role__c",
  
  -- Preferred Language: map to enum values
  CASE 
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
    WHEN UPPER(TRIM(c.korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
    ELSE NULL
  END AS "Preferred_Language__c",
  
  -- AccountId: join to customer using kd_nummer
  CASE 
    WHEN c.kd_nummer LIKE 'CUST-M%' THEN am.account_id
    ELSE NULL
  END AS "AccountId",
  
  -- Legacy Contact ID from source natural key
  c.kontakt_id AS "Legacy_Contact_ID__c",
  
  -- Timestamps
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- Not deleted
  0 AS "IsDeleted"

FROM contact_data c
LEFT JOIN account_mapping am ON c.kd_nummer = am.kundennummer
