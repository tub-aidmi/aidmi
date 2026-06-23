{{
  config(
    materialized='table'
  )
}}

WITH opportunity_source AS (
  SELECT
    opp_kennung,
    titel,
    vertriebsphase,
    zieldatum,
    auftragswert,
    waehrungscode,
    kunden_ref
  FROM {{ source('fixture_master_src', 'master_opportunities') }}
),

-- Map vertriebsphase to StageName enum
stage_mapping AS (
  SELECT
    opp_kennung,
    titel,
    CASE
      WHEN LOWER(TRIM(vertriebsphase)) = 'gewonnen' THEN 'Closed Won'
      WHEN LOWER(TRIM(vertriebsphase)) = 'prospecting' THEN 'Prospecting'
      WHEN LOWER(TRIM(vertriebsphase)) = 'qualification' THEN 'Qualification'
      WHEN LOWER(TRIM(vertriebsphase)) = 'needs analysis' THEN 'Needs Analysis'
      WHEN LOWER(TRIM(vertriebsphase)) = 'value proposition' THEN 'Value Proposition'
      WHEN LOWER(TRIM(vertriebsphase)) = 'id. decision makers' THEN 'Id. Decision Makers'
      WHEN LOWER(TRIM(vertriebsphase)) = 'perception analysis' THEN 'Perception Analysis'
      WHEN LOWER(TRIM(vertriebsphase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
      WHEN LOWER(TRIM(vertriebsphase)) = 'negotiation/review' THEN 'Negotiation/Review'
      WHEN LOWER(TRIM(vertriebsphase)) = 'closed lost' THEN 'Closed Lost'
      ELSE 'Prospecting'
    END AS stage_name,
    zieldatum,
    auftragswert,
    waehrungscode,
    kunden_ref
  FROM opportunity_source
),

-- Parse and standardize CloseDate
date_parsed AS (
  SELECT
    opp_kennung,
    titel,
    stage_name,
    CASE
      WHEN zieldatum IS NULL OR TRIM(zieldatum) = 'N/A' THEN NULL
      WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
      WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
        TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS close_date,
    auftragswert,
    waehrungscode,
    kunden_ref
  FROM stage_mapping
),

-- Parse Amount and standardize CurrencyIsoCode
amount_currency_parsed AS (
  SELECT
    opp_kennung,
    titel,
    stage_name,
    close_date,
    CASE
      WHEN auftragswert IS NULL THEN NULL
      WHEN auftragswert ~ '^[0-9]+(\.[0-9]+)?$' THEN CAST(auftragswert AS DOUBLE PRECISION)
      WHEN auftragswert ~ '^[A-Za-z]+ [0-9]+(\.[0-9]+)?$' THEN
        CAST(REGEXP_REPLACE(auftragswert, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
      ELSE NULL
    END AS amount,
    CASE
      WHEN LOWER(TRIM(waehrungscode)) = 'chf' THEN 'CHF'
      WHEN LOWER(TRIM(waehrungscode)) = 'dollar' THEN 'USD'
      WHEN LOWER(TRIM(waehrungscode)) = '€' THEN 'EUR'
      WHEN LOWER(TRIM(waehrungscode)) = 'eur' THEN 'EUR'
      ELSE UPPER(TRIM(waehrungscode))
    END AS currency_iso_code,
    kunden_ref
  FROM date_parsed
),

-- Map kunden_ref to AccountId (replace 'KD-M' with 'CUST-M')
account_mapped AS (
  SELECT
    opp_kennung,
    titel,
    stage_name,
    close_date,
    amount,
    currency_iso_code,
    REPLACE(kunden_ref, 'KD-M', 'CUST-M') AS account_id
  FROM amount_currency_parsed
)

SELECT
  opp_kennung AS Id,
  COALESCE(NULLIF(TRIM(titel), ''), 'Untitled Opportunity') AS Name,
  stage_name AS StageName,
  close_date AS CloseDate,
  amount AS Amount,
  currency_iso_code AS CurrencyIsoCode,
  account_id AS AccountId,
  opp_kennung AS Legacy_Opportunity_ID__c,
  NULL AS CreatedDate,
  NULL AS LastModifiedDate,
  0 AS IsDeleted
FROM account_mapped