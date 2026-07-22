{{ config(materialized='table') }}
WITH opportunity_source AS (
  SELECT 
    opp.opp_kennung,
    opp.titel,
    opp.vertriebsphase,
    opp.zieldatum,
    opp.auftragswert,
    opp.waehrungscode,
    k.kundennummer AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON REPLACE(opp.kunden_ref, 'KD-', 'CUST-') = k.kundennummer
)
SELECT 
  opp_kennung AS "Id",
  titel AS "Name",
  CASE 
    WHEN LOWER(TRIM(vertriebsphase)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('needs analysis', 'in prüfung') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('value proposition') THEN 'Value Proposition'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('id. decision makers', 'perception analysis') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('negotiation/review') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('closed won', 'gewonnen', 'won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
    WHEN LOWER(TRIM(vertriebsphase)) IN ('closed lost', 'verloren', 'lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
    ELSE NULL 
  END AS "StageName",
  CASE 
    WHEN zieldatum ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND TO_DATE(zieldatum, 'YYYY-MM-DD') IS NOT NULL THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
    WHEN zieldatum ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' AND TO_DATE(zieldatum, 'DD.MM.YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN zieldatum ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' AND TO_DATE(zieldatum, 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN zieldatum ~ '^[0-9]{8}$' AND TO_DATE(zieldatum, 'YYYYMMDD') IS NOT NULL THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL 
  END AS "CloseDate",
  CASE 
    WHEN auftragswert ~ '^[0-9]+\.[0-9]{3}(\.[0-9]{3})*(,[0-9]+)?$' THEN 
      CAST(REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '\.', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
    WHEN auftragswert ~ '^[0-9]+(,[0-9]+)?$' THEN 
      CAST(REGEXP_REPLACE(auftragswert, ',', '.', 'g') AS DOUBLE PRECISION)
    WHEN auftragswert ~ '^[0-9]+\.[0-9]+$' THEN 
      CAST(auftragswert AS DOUBLE PRECISION)
    ELSE NULL 
  END AS "Amount",
  CASE 
    WHEN LOWER(TRIM(waehrungscode)) IN ('chf') THEN 'CHF'
    WHEN LOWER(TRIM(waehrungscode)) IN ('eur', 'euro', '€') THEN 'EUR'
    WHEN LOWER(TRIM(waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
    WHEN LOWER(TRIM(waehrungscode)) IN ('gbp', '£') THEN 'GBP'
    ELSE UPPER(TRIM(waehrungscode)) 
  END AS "CurrencyIsoCode",
  account_id AS "AccountId",
  opp_kennung AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM opportunity_source