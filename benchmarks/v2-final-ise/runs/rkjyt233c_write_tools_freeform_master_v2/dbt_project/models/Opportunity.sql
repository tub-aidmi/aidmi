{{ config(materialized='table') }}
WITH opportunity_data AS (
  SELECT
    o.opp_kennung,
    TRIM(o.titel) AS name,
    o.vertriebsphase AS stage,
    o.zieldatum AS close_date,
    o.auftragswert AS amount,
    o.waehrungscode AS currency_code,
    o.kunden_ref AS customer_ref
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
),
normalized_opportunities AS (
  SELECT
    opp_kennung,
    name,
    -- Normalize stage to enum
    CASE
      WHEN UPPER(TRIM(stage)) IN ('PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
      WHEN UPPER(TRIM(stage)) IN ('QUALIFICATION', 'QUALI') THEN 'Qualification'
      WHEN UPPER(TRIM(stage)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
      WHEN UPPER(TRIM(stage)) IN ('VALUE PROPOSITION', 'WERTVORSCHLAG') THEN 'Value Proposition'
      WHEN UPPER(TRIM(stage)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
      WHEN UPPER(TRIM(stage)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
      WHEN UPPER(TRIM(stage)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT', 'ANGEBOTSERSTELLUNG') THEN 'Proposal/Price Quote'
      WHEN UPPER(TRIM(stage)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG', 'VERHANDLUNGEN') THEN 'Negotiation/Review'
      WHEN UPPER(TRIM(stage)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
      WHEN UPPER(TRIM(stage)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN') THEN 'Closed Lost'
      ELSE NULL
    END AS stage,
    -- Parse close_date: handle YYYY-MM-DD, DD.MM.YYYY, MM/DD/YYYY, YYYYMMDD
    CASE
      WHEN close_date IS NULL OR TRIM(close_date) = '' THEN NULL
      WHEN TRIM(close_date) = '0000-00-00' THEN NULL
      WHEN TRIM(close_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CASE WHEN TO_DATE(TRIM(close_date), 'YYYY-MM-DD') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(close_date), 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(close_date) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
        CASE WHEN TO_DATE(TRIM(close_date), 'DD.MM.YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(close_date), 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(close_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        CASE WHEN TO_DATE(TRIM(close_date), 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(close_date), 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(close_date) ~ '^\d{8}$' THEN
        CASE WHEN TO_DATE(TRIM(close_date), 'YYYYMMDD') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(close_date), 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END
      ELSE NULL
    END AS close_date,
    -- Parse amount: handle EUR 144893.69, 116121.28, None, EUR 368803.66
    CASE
      WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' OR UPPER(TRIM(amount)) = 'NONE' THEN NULL
      ELSE
        -- Remove all non-numeric characters except dots and commas
        REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(amount), '[^0-9.,-]', '', 'g'),
          '^[+-]?', '', 'g'
        )
    END AS raw_amount,
    -- Normalize currency code to ISO
    CASE
      WHEN UPPER(TRIM(currency_code)) IN ('EUR', '€') THEN 'EUR'
      WHEN UPPER(TRIM(currency_code)) IN ('USD', '$') THEN 'USD'
      WHEN UPPER(TRIM(currency_code)) IN ('CHF', 'CHF ') THEN 'CHF'
      WHEN UPPER(TRIM(currency_code)) IN ('GBP', '£') THEN 'GBP'
      ELSE UPPER(TRIM(currency_code))
    END AS currency_code,
    customer_ref
  FROM opportunity_data o
),
amount_parsed AS (
  SELECT
    opp_kennung,
    name,
    stage,
    close_date,
    currency_code,
    customer_ref,
    -- Handle European and US number formats
    CASE
      WHEN raw_amount IS NULL THEN NULL
      -- European format: 1.234,56 -> 1234.56
      WHEN raw_amount ~ '^\d{1,3}(\.\d{3})*\,\d+$' THEN CAST(REPLACE(REPLACE(raw_amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
      -- US format: 1,234.56 -> 1234.56
      WHEN raw_amount ~ '^\d{1,3}(,\d{3})*\.\d+$' THEN CAST(REPLACE(raw_amount, ',', '') AS DOUBLE PRECISION)
      -- Plain number with comma or dot
      WHEN raw_amount ~ '^\d+,\d+$' THEN CAST(REPLACE(raw_amount, ',', '.') AS DOUBLE PRECISION)
      WHEN raw_amount ~ '^\d+\.\d+$' THEN CAST(raw_amount AS DOUBLE PRECISION)
      WHEN raw_amount ~ '^\d+$' THEN CAST(raw_amount AS DOUBLE PRECISION)
      ELSE NULL
    END AS amount
  FROM normalized_opportunities
)
SELECT
  opp_kennung AS "Id",
  COALESCE(NULLIF(name, ''), 'Untitled Opportunity') AS "Name",
  COALESCE(stage, 'Prospecting') AS "StageName",
  COALESCE(close_date, (CURRENT_DATE + INTERVAL '30 days')::TEXT) AS "CloseDate",
  amount AS "Amount",
  currency_code AS "CurrencyIsoCode",
  -- Map customer_ref (KD-M*) to Account Id (CUST-M*)
  REPLACE(customer_ref, 'KD-M', 'CUST-M') AS "AccountId",
  opp_kennung AS "Legacy_Opportunity_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM amount_parsed