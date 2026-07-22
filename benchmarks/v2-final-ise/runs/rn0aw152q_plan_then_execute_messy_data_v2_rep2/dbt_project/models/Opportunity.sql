{{ config(materialized='table') }}

SELECT
  opp.id AS "Id",
  COALESCE(TRIM(INITCAP(opp.name)), 'Unknown') AS "Name",
  CASE LOWER(TRIM(COALESCE(opp.stagename, '')))
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
    WHEN TRIM(opp.closedate) = '' OR opp.closedate IS NULL THEN NULL
    WHEN TRIM(opp.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(opp.closedate), 'YYYY-MM-DD')::TEXT
    WHEN TRIM(opp.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(opp.closedate), 'DD.MM.YYYY')::TEXT
    WHEN TRIM(opp.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(opp.closedate), 'MM/DD/YYYY')::TEXT
    ELSE NULL
  END AS "CloseDate",
  CASE
    WHEN TRIM(COALESCE(opp.amount, '')) = '' THEN NULL
    WHEN TRIM(opp.amount) ~ '^\d{1,3}(\.\d{3})+(,\d+)$' THEN
      CAST(REPLACE(REPLACE(TRIM(opp.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
    WHEN TRIM(opp.amount) ~ '^[+-]?[0-9]+(\.[0-9]+)?$' THEN
      CAST(REGEXP_REPLACE(TRIM(opp.amount), '[^\d.\-]', '', 'g') AS DOUBLE PRECISION)
    WHEN TRIM(opp.amount) ~ '^[+-]?\d+,\d+$' THEN
      CAST(REPLACE(TRIM(opp.amount), ',', '.') AS DOUBLE PRECISION)
    ELSE NULL
  END AS "Amount",
  UPPER(TRIM(COALESCE(opp.currencyisocode, ''))) AS "CurrencyIsoCode",
  acct.id AS "AccountId",
  opp.id AS "Legacy_Opportunity_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} opp
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acct
  ON TRIM(opp.accountid) = COALESCE(TRIM(acct.erp_number__c), '')
     OR TRIM(opp.accountid) = COALESCE(TRIM(acct.legacy_customer_id__c), '');