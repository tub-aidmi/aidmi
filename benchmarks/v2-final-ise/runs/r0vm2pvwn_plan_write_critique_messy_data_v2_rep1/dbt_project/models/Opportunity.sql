{{ config(materialized='table') }}

WITH src_opportunity AS (
  SELECT * FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
src_account AS (
  SELECT id
  FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT 
  o.id AS "Id",
  COALESCE(TRIM(o.name), 'Untitled') AS "Name",
  CASE 
    WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
    WHEN LOWER(TRIM(o.stagename)) IN ('qualification', 'qualifikation', 'in prüfung', 'quali') THEN 'Qualification'
    WHEN LOWER(TRIM(o.stagename)) = 'needs analysis' THEN 'Needs Analysis'
    WHEN LOWER(TRIM(o.stagename)) = 'value proposition' THEN 'Value Proposition'
    WHEN LOWER(TRIM(o.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(o.stagename)) = 'perception analysis' THEN 'Perception Analysis'
    WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal / price quote') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation / review') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
    WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  CASE 
    WHEN o.closedate IS NULL OR TRIM(o.closedate) = '' THEN NULL
    WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN o.closedate ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN o.closedate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN o.closedate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN o.closedate
    ELSE NULL
  END AS "CloseDate",
  CASE 
    WHEN TRIM(LOWER(o.amount)) IN ('none', '', 'null', 'n/a') THEN NULL
    ELSE
      CASE 
        -- European format: both dot and comma present (dot=thousands, comma=decimal)
        WHEN o.amount ~ '\.' AND o.amount ~ ',' THEN
          CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(o.amount), '(USD|EUR|GBP|CHF|\$|£|€)', '', 'gi'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        -- Only comma present: treat as decimal separator
        WHEN o.amount ~ ',' THEN
          CAST(REPLACE(REGEXP_REPLACE(TRIM(o.amount), '(USD|EUR|GBP|CHF|\$|£|€)', '', 'gi'), ',', '.') AS DOUBLE PRECISION)
        -- No comma pattern issues: strip currency text and cast
        ELSE
          CAST(REGEXP_REPLACE(TRIM(o.amount), '(USD|EUR|GBP|CHF|\$|£|€)', '', 'gi') AS DOUBLE PRECISION)
      END
  END AS "Amount",
  CASE 
    WHEN UPPER(TRIM(o.currencyisocode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
    WHEN UPPER(TRIM(o.currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
    WHEN UPPER(TRIM(o.currencyisocode)) IN ('GBP', '£') THEN 'GBP'
    WHEN UPPER(TRIM(o.currencyisocode)) IN ('CHF') THEN 'CHF'
    ELSE UPPER(TRIM(o.currencyisocode))
  END AS "CurrencyIsoCode",
  a.id AS "AccountId",
  o.id AS "Legacy_Opportunity_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM src_opportunity o
LEFT JOIN src_account a 
  ON TRIM(o.accountid) = TRIM(a.id)