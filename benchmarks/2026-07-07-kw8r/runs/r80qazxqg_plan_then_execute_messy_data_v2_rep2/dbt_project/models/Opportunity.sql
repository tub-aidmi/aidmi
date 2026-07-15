{{ config(materialized='table') }}

WITH source_data AS (
  SELECT * FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT 
  id AS "Id",
  COALESCE(TRIM(INITCAP(name)), 'Unknown') AS "Name",
  CASE LOWER(TRIM(COALESCE(stagename, '')))
    WHEN 'prospecting' THEN 'Prospecting'
    WHEN 'qualification' THEN 'Qualification'
    WHEN 'needs analysis' THEN 'Needs Analysis'
    WHEN 'value proposition' THEN 'Value Proposition'
    WHEN 'id. decision makers' THEN 'Id. Decision Makers'
    WHEN 'perception analysis' THEN 'Perception Analysis'
    WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
    WHEN 'negotiation/review' THEN 'Negotiation/Review'
    WHEN 'closed won' THEN 'Closed Won'
    WHEN 'closed lost' THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  CASE 
    WHEN closedate IS NULL OR TRIM(closedate) = '' THEN CURRENT_DATE::TEXT
    WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
    WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
    WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
    ELSE CURRENT_DATE::TEXT
  END AS "CloseDate",
  CASE 
    WHEN COALESCE(TRIM(amount), '') = '' THEN NULL
    WHEN COALESCE(TRIM(amount), '') ~ ',' THEN 
      CAST(REPLACE(REPLACE(COALESCE(TRIM(amount), ''), '.', ''), ',', '.') AS DOUBLE PRECISION)
    ELSE 
      CAST(REGEXP_REPLACE(COALESCE(TRIM(amount), ''), '[^\d.]', '', 'g') AS DOUBLE PRECISION)
  END AS "Amount",
  UPPER(TRIM(COALESCE(currencyisocode, ''))) AS "CurrencyIsoCode",
  accountid AS "AccountId",
  id AS "Legacy_Opportunity_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM source_data;