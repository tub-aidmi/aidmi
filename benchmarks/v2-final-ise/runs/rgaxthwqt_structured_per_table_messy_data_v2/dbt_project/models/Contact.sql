{{ config(materialized='table') }}

WITH accounts AS (
  SELECT
    id AS sf_account_id,
    COALESCE(legacy_customer_id__c, erp_number__c, LOWER(name)) AS legacy_key
  FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),
cleaned_contact AS (
  SELECT
    -- Id: deterministic hash for Salesforce-style ID
    CAST(MD5(id) AS text) AS "Id",
    -- Legacy contact id from source natural key
    COALESCE(TRIM(id), '') AS "Legacy_Contact_ID__c",
    -- Contact personal info (trim whitespace)
    TRIM(firstname) AS "FirstName",
    -- LastName is NOT NULL in target; coalesce to 'Unknown' if empty/NULL
    COALESCE(NULLIF(TRIM(lastname), ''), 'Unknown') AS "LastName",
    -- Email lowercased for consistency
    LOWER(TRIM(email)) AS "Email",
    -- Phone cleaned of spaces, dashes, parentheses — keep digits, +, - for intl
    REGEXP_REPLACE(TRIM(phone), '[^0-9+()\-]', '', 'g') AS "Phone",
    -- Title normalized with INITCAP
    INITCAP(TRIM(title)) AS "Title",
    -- Role__c: map source values to declared enum domain
    CASE
      WHEN LOWER(TRIM(role__c)) IN ('decision maker', 'decider', 'dm') THEN 'Decision Maker'
      WHEN LOWER(TRIM(role__c)) IN ('end user', 'user', 'final user') THEN 'End User'
      WHEN LOWER(TRIM(role__c)) IN ('technical contact', 'tech contact', 'technical', 'it contact') THEN 'Technical Contact'
      WHEN LOWER(TRIM(role__c)) IN ('executive sponsor', 'exec sponsor', 'esponsor', 'sponsor', 'executive') THEN 'Executive Sponsor'
      ELSE NULL
    END AS "Role__c",
    -- Preferred_Language__c: uppercase, normalize to declared enum values
    CASE UPPER(TRIM(preferred_language__c))
      WHEN 'DE' THEN 'DE'
      WHEN 'EN' THEN 'EN'
      WHEN 'FR' THEN 'FR'
      WHEN 'ES' THEN 'ES'
      WHEN 'IT' THEN 'IT'
      ELSE NULL
    END AS "Preferred_Language__c",
    -- Raw accountid for joining with Account.Legacy_Customer_ID__c
    TRIM(accountid) AS _account_legacy_key
  FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
)

SELECT
  cc."Id",
  cc."FirstName",
  cc."LastName",
  cc."Email",
  cc."Phone",
  cc."Title",
  cc."Role__c",
  cc."Preferred_Language__c",
  accounts.sf_account_id AS "AccountId",
  cc."Legacy_Contact_ID__c",
  -- Static defaults for audit columns not present in source
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM cleaned_contact cc
LEFT JOIN accounts
  ON LOWER(cc._account_legacy_key) = LOWER(accounts.legacy_key)