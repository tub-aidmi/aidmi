{{ config(materialized='table') }}

WITH opportunity_source AS (
  SELECT * FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
account_mapping AS (
  SELECT 
    "kundennummer" AS account_id,
    "kundennummer" AS legacy_customer_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
  opp."opp_kennung" AS "Id",
  opp."titel" AS "Name",
  CASE
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('prospecting') THEN 'Prospecting'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('qualification', 'quali') THEN 'Qualification'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('needs analysis') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('value proposition') THEN 'Value Proposition'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('perception analysis') THEN 'Perception Analysis'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('closed won', 'abgeschlossen (gewonnen)', 'closed won') THEN 'Closed Won'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('in kontakt') THEN 'Prospecting'
    ELSE 'Prospecting'
  END AS "StageName",
  
  CASE
    WHEN opp."zieldatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN opp."zieldatum"
    WHEN opp."zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp."zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN opp."zieldatum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp."zieldatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN opp."zieldatum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp."zieldatum", 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "CloseDate",
  
  CASE
    WHEN opp."auftragswert" = 'None' OR opp."auftragswert" IS NULL THEN NULL
    WHEN opp."auftragswert" ~ '^[0-9]+\.?[0-9]*$' THEN opp."auftragswert"::DOUBLE PRECISION
    WHEN opp."auftragswert" ~ '^[0-9]+,[0-9]+$' THEN REPLACE(opp."auftragswert", ',', '.')::DOUBLE PRECISION
    WHEN opp."auftragswert" ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(opp."auftragswert", '.', ''), ',', '.')::DOUBLE PRECISION
    WHEN LOWER(TRIM(opp."auftragswert")) ~ '^eur [0-9]+\.?[0-9]*$' THEN REGEXP_REPLACE(LOWER(TRIM(opp."auftragswert")), '^eur ', '')::DOUBLE PRECISION
    WHEN LOWER(TRIM(opp."auftragswert")) ~ '^chf [0-9]+\.?[0-9]*$' THEN REGEXP_REPLACE(LOWER(TRIM(opp."auftragswert")), '^chf ', '')::DOUBLE PRECISION
    WHEN LOWER(TRIM(opp."auftragswert")) ~ '^\$[0-9]+\.?[0-9]*$' THEN REGEXP_REPLACE(LOWER(TRIM(opp."auftragswert")), '^\$', '')::DOUBLE PRECISION
    WHEN LOWER(TRIM(opp."auftragswert")) ~ '^€[0-9]+\.?[0-9]*$' THEN REGEXP_REPLACE(LOWER(TRIM(opp."auftragswert")), '^€', '')::DOUBLE PRECISION
    ELSE NULL
  END AS "Amount",
  
  CASE
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('eur', '€') THEN 'EUR'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('chf') THEN 'CHF'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('usd', '$', 'dollar') THEN 'USD'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('gbp', '£') THEN 'GBP'
    ELSE UPPER(TRIM(opp."waehrungscode"))
  END AS "CurrencyIsoCode",
  
  REPLACE(opp."kunden_ref", 'KD-', 'CUST-') AS "AccountId",
  opp."opp_kennung" AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM opportunity_source opp