{{ config(materialized='table') }}
WITH opportunity_cleaned AS (
  SELECT
    o.id,
    o.name,
    -- Normalize StageName to target enum
    CASE
      WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
      WHEN LOWER(TRIM(o.stagename)) IN ('qualification') THEN 'Qualification'
      WHEN LOWER(TRIM(o.stagename)) IN ('needs analysis') THEN 'Needs Analysis'
      WHEN LOWER(TRIM(o.stagename)) IN ('value proposition') THEN 'Value Proposition'
      WHEN LOWER(TRIM(o.stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
      WHEN LOWER(TRIM(o.stagename)) IN ('perception analysis') THEN 'Perception Analysis'
      WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
      WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
      WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won') THEN 'Closed Won'
      WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
      ELSE NULL
    END AS stagename,
    -- Parse CloseDate to ISO format
    CASE
      WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN o.closedate ~ '^\d{1}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'M/DD/YYYY'), 'YYYY-MM-DD')
      WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
      ELSE NULL
    END AS closedate,
    -- Clean and cast Amount, handling empty strings, currency symbols, and European formats
    CASE
      WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
      WHEN o.amount ~ '^[+-]?[0-9]+(\.[0-9]+)?$' THEN o.amount::DOUBLE PRECISION
      ELSE
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(o.amount, '[^0-9.,+-]', '', 'g'),
              '\.', '', 'g'
            ),
            ',', '.', 'g'
          ),
          '^\+?', '', 'g'
        )::DOUBLE PRECISION
    END AS amount,
    -- Normalize CurrencyIsoCode
    CASE
      WHEN LOWER(TRIM(o.currencyisocode)) IN ('eur', 'euro') THEN 'EUR'
      WHEN LOWER(TRIM(o.currencyisocode)) IN ('usd', 'dollar') THEN 'USD'
      WHEN LOWER(TRIM(o.currencyisocode)) IN ('chf') THEN 'CHF'
      WHEN LOWER(TRIM(o.currencyisocode)) IN ('£', 'gbp') THEN 'GBP'
      ELSE o.currencyisocode
    END AS currencyisocode,
    o.accountid
  FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
)
SELECT
  oc.id AS "Id",
  COALESCE(NULLIF(TRIM(oc.name), ''), 'Unknown') AS "Name",
  COALESCE(oc.stagename, 'Prospecting') AS "StageName",
  COALESCE(oc.closedate, NULL) AS "CloseDate",
  oc.amount AS "Amount",
  oc.currencyisocode AS "CurrencyIsoCode",
  a.id AS "AccountId",
  oc.id AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM opportunity_cleaned oc
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON oc.accountid = a.id