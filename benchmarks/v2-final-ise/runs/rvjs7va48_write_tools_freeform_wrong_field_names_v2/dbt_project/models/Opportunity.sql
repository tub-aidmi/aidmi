{{ config(materialized='table') }}

WITH opportunity_data AS (
  SELECT
    chance_id AS legacy_opportunity_id,
    bezeichnung,
    phase,
    abschlussdatum,
    volumen,
    waehrung,
    kd_nr AS customer_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
  legacy_opportunity_id AS "Id",
  INITCAP(TRIM(bezeichnung)) AS "Name",
  CASE 
    WHEN UPPER(TRIM(phase)) IN ('PROSPEKTIERUNG', 'PROSPECTING') THEN 'Prospecting'
    WHEN UPPER(TRIM(phase)) IN ('QUALIFIZIERUNG', 'QUALIFICATION') THEN 'Qualification'
    WHEN UPPER(TRIM(phase)) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS') THEN 'Needs Analysis'
    WHEN UPPER(TRIM(phase)) IN ('WERTVORSCHLAG', 'VALUE PROPOSITION') THEN 'Value Proposition'
    WHEN UPPER(TRIM(phase)) IN ('ENTSCHEIDUNGSTRÄGER IDENTIFIZIERT', 'ID. DECISION MAKERS') THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(phase)) IN ('WAHRNEHMUNGSANALYSE', 'PERCEPTION ANALYSIS') THEN 'Perception Analysis'
    WHEN UPPER(TRIM(phase)) IN ('ANGEBOT/PREIS', 'PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(phase)) IN ('VERHANDLUNG/REVIEW', 'NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(phase)) IN ('GESCHLOSSEN GEWONNEN', 'CLOSED WON') THEN 'Closed Won'
    WHEN UPPER(TRIM(phase)) IN ('GESCHLOSSEN VERLOREN', 'CLOSED LOST') THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  CASE 
    WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
    WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "CloseDate",
  volumen AS "Amount",
  waehrung AS "CurrencyIsoCode",
  customer_id AS "AccountId",
  legacy_opportunity_id AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM opportunity_data