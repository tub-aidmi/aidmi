{%
  config(
    materialized='table'
  )
%}

WITH opportunity_source AS (
  SELECT
    opp_kennung AS legacy_opportunity_id,
    titel AS name,
    INITCAP(TRIM(vertriebsphase)) AS stage_name,
    zieldatum AS close_date,
    auftragswert AS amount_raw,
    waehrungscode AS currency_iso_code,
    kunden_ref AS legacy_customer_id
  FROM {{ source('fixture_master_src', 'master_opportunities') }}
)

SELECT
  opp.opp_kennung AS Id,
  COALESCE(opp.titel, 'Untitled Opportunity') AS Name,
  CASE
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Gewonnen' THEN 'Closed Won'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Prospecting' THEN 'Prospecting'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Qualification' THEN 'Qualification'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Needs Analysis' THEN 'Needs Analysis'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Value Proposition' THEN 'Value Proposition'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Perception Analysis' THEN 'Perception Analysis'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Negotiation/Review' THEN 'Negotiation/Review'
    WHEN INITCAP(TRIM(opp.vertriebsphase)) = 'Closed Lost' THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS StageName,
  COALESCE(
    NULLIF(opp.zieldatum, 'N/A'),
    NULLIF(opp.zieldatum, '')
  ) AS CloseDate,
  CASE
    WHEN opp.auftragswert ~ '^[0-9]+(\.[0-9]+)?$' THEN CAST(opp.auftragswert AS DOUBLE PRECISION)
    WHEN opp.auftragswert ~ '^[A-Z]{3} [0-9]+(\.[0-9]+)?$' THEN CAST(REGEXP_REPLACE(opp.auftragswert, '^[A-Z]{3} ', '') AS DOUBLE PRECISION)
    WHEN opp.auftragswert ~ '^[€$£¥]?[0-9]+(\.[0-9]+)?$' THEN CAST(REGEXP_REPLACE(opp.auftragswert, '[^0-9.]', '') AS DOUBLE PRECISION)
    ELSE NULL
  END AS Amount,
  CASE
    WHEN UPPER(TRIM(opp.waehrungscode)) IN ('EUR', '€') THEN 'EUR'
    WHEN UPPER(TRIM(opp.waehrungscode)) IN ('CHF') THEN 'CHF'
    WHEN UPPER(TRIM(opp.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
    WHEN UPPER(TRIM(opp.waehrungscode)) IN ('GBP', '£') THEN 'GBP'
    WHEN UPPER(TRIM(opp.waehrungscode)) IN ('JPY', '¥') THEN 'JPY'
    ELSE NULL
  END AS CurrencyIsoCode,
  cust.kundennummer AS AccountId,
  opp.opp_kennung AS Legacy_Opportunity_ID__c,
  NULL::text AS CreatedDate,
  NULL::text AS LastModifiedDate,
  0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} cust
  ON opp.kunden_ref = cust.kundennummer