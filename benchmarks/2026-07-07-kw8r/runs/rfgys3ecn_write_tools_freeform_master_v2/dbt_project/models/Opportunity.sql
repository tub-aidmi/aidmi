{{ config(materialized='table') }}
WITH source_data AS (
  SELECT
    opp_kennung,
    titel,
    vertriebsphase,
    zieldatum,
    auftragswert,
    waehrungscode,
    kunden_ref
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
parsed_opportunities AS (
  SELECT
    opp_kennung,
    titel,
    vertriebsphase,
    zieldatum,
    -- Clean and parse amount
    CASE
      WHEN auftragswert ~ '^[0-9\.,]+$' THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(auftragswert, '[.]', '', 'g'),
          ',', '.', 'g'
        )::DOUBLE PRECISION
      ELSE NULL
    END AS amount_clean,
    waehrungscode,
    kunden_ref
  FROM source_data
)
SELECT
  gen_random_uuid()::text AS "Id",
  INITCAP(TRIM(titel)) AS "Name",
  CASE
    WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPEKTING', 'PROSPEKTION') THEN 'Prospecting'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFICATION', 'QUALIFIZIERUNG') THEN 'Qualification'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('VALUE PROPOSITION', 'WERTANGEBOT') THEN 'Value Proposition'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT/PREISANGEBOT') THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG/PRÜFUNG') THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED WON', 'GESCHLOSSEN GEWONNEN') THEN 'Closed Won'
    WHEN UPPER(TRIM(vertriebsphase)) IN ('CLOSED LOST', 'GESCHLOSSEN VERLOREN') THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  CASE
    WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
    WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "CloseDate",
  amount_clean AS "Amount",
  UPPER(TRIM(waehrungscode)) AS "CurrencyIsoCode",
  CASE WHEN TRIM(kunden_ref) IS NOT NULL THEN md5('ns:' || TRIM(kunden_ref)) ELSE NULL END AS "AccountId",
  TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM parsed_opportunities