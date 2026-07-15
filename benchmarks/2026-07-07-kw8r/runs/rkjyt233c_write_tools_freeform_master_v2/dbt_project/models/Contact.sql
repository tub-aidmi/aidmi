{{ config(materialized='table') }}

WITH contact_data AS (
  SELECT
    k.kontakt_id,
    k.rufname AS first_name,
    k.familienname AS last_name,
    k.kontakt_email AS email,
    k.tel AS phone,
    k.berufsbezeichnung AS title,
    k.rolle AS role,
    k.korrespondenzsprache AS language,
    k.kd_nummer AS customer_id
  FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
),
normalized_contacts AS (
  SELECT
    kontakt_id,
    INITCAP(TRIM(first_name)) AS first_name,
    INITCAP(TRIM(last_name)) AS last_name,
    LOWER(TRIM(email)) AS email,
    TRIM(phone) AS phone,
    INITCAP(TRIM(title)) AS title,
    -- Normalize role to enum
    CASE 
      WHEN UPPER(TRIM(role)) IN ('DECISION MAKER', 'ENTSCHEIDUNGSTRÄGER') THEN 'Decision Maker'
      WHEN UPPER(TRIM(role)) IN ('END USER', 'ENDNUTZER', 'BENUTZER') THEN 'End User'
      WHEN UPPER(TRIM(role)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHER KONTAKT') THEN 'Technical Contact'
      WHEN UPPER(TRIM(role)) IN ('EXECUTIVE SPONSOR', 'EXEKUTIVER SPONSOR') THEN 'Executive Sponsor'
      ELSE NULL
    END AS role,
    -- Normalize language to 2-letter codes
    CASE 
      WHEN UPPER(TRIM(language)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
      WHEN UPPER(TRIM(language)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
      WHEN UPPER(TRIM(language)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
      WHEN UPPER(TRIM(language)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
      WHEN UPPER(TRIM(language)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
      ELSE NULL
    END AS language,
    customer_id
  FROM contact_data
)
SELECT
  kontakt_id AS "Id",
  first_name AS "FirstName",
  COALESCE(NULLIF(last_name, ''), 'Unknown') AS "LastName",
  email AS "Email",
  phone AS "Phone",
  title AS "Title",
  role AS "Role__c",
  language AS "Preferred_Language__c",
  customer_id AS "AccountId",
  kontakt_id AS "Legacy_Contact_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM normalized_contacts
