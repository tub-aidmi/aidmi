{{ config(materialized='table') }}
WITH opportunity_cleaned AS (
  SELECT
    id,
    name,
    -- Normalize StageName to target enum
    CASE
      WHEN LOWER(TRIM(stagename)) IN ('closed won', 'closedwon') THEN 'Closed Won'
      WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'closedlost', 'lost') THEN 'Closed Lost'
      WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
      WHEN LOWER(TRIM(stagename)) IN ('qualification') THEN 'Qualification'
      WHEN LOWER(TRIM(stagename)) IN ('needs analysis') THEN 'Needs Analysis'
      WHEN LOWER(TRIM(stagename)) IN ('value proposition') THEN 'Value Proposition'
      WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
      WHEN LOWER(TRIM(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
      WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
      WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
      ELSE 'Prospecting'
    END AS "StageName",
    -- Parse CloseDate to ISO format
    CASE
      WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
      ELSE NULL
    END AS "CloseDate",
    -- Clean and parse Amount
    CASE
      WHEN TRIM(amount) = 'None' THEN NULL
      WHEN amount ~ '^[0-9]+\.[0-9],[0-9]{2}$' THEN REGEXP_REPLACE(REGEXP_REPLACE(amount, '\.', '', 'g'), ',', '.')::DOUBLE PRECISION
      WHEN amount ~ '^[0-9]+,[0-9]{2}$' THEN REGEXP_REPLACE(amount, ',', '.')::DOUBLE PRECISION
      WHEN amount ~ '^[A-Za-z\s]+[0-9]+\.[0-9]{1,2}$' THEN REGEXP_REPLACE(amount, '[^0-9.]', '', 'g')::DOUBLE PRECISION
      WHEN amount ~ '^[0-9]+\.[0-9]{1,2}$' THEN amount::DOUBLE PRECISION
      WHEN amount ~ '^[0-9]+$' THEN amount::DOUBLE PRECISION
      WHEN amount ~ '^-[0-9]+\.[0-9]{1,2}$' THEN amount::DOUBLE PRECISION
      ELSE NULL
    END AS "Amount",
    -- Normalize CurrencyIsoCode
    CASE
      WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
      WHEN LOWER(TRIM(currencyisocode)) IN ('usd', 'dollar', '$') THEN 'USD'
      WHEN LOWER(TRIM(currencyisocode)) IN ('chf', 'swiss franc') THEN 'CHF'
      WHEN LOWER(TRIM(currencyisocode)) IN ('£', 'gbp') THEN 'GBP'
      ELSE UPPER(TRIM(currencyisocode))
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c"
  FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
  id AS "Id",
  COALESCE(name, 'Unknown') AS "Name",
  "StageName",
  "CloseDate",
  "Amount",
  "CurrencyIsoCode",
  "AccountId",
  "Legacy_Opportunity_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM opportunity_cleaned