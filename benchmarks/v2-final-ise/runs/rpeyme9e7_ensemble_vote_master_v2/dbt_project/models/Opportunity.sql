{{ config(materialized='table') }}

SELECT 
  REPLACE(opp."opp_kennung", 'OPP-', 'OPP_') AS "Id",
  opp."titel" AS "Name",
  CASE 
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('prospecting', 'in kontakt') THEN 'Prospecting'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('qualification', 'quali') THEN 'Qualification'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('needs analysis') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('value proposition') THEN 'Value Proposition'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('id. decision makers') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('perception analysis') THEN 'Perception Analysis'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('negotiation/review') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
    WHEN LOWER(TRIM(opp."vertriebsphase")) IN ('closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  CASE 
    WHEN opp."zieldatum" IS NULL THEN NULL
    WHEN opp."zieldatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN opp."zieldatum"
    WHEN opp."zieldatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
      TO_CHAR(TO_DATE(opp."zieldatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN opp."zieldatum" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(TO_DATE(opp."zieldatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN opp."zieldatum" ~ '^\d{8}$' THEN 
      TO_CHAR(TO_DATE(opp."zieldatum", 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL 
  END AS "CloseDate",
  CASE 
    WHEN opp."auftragswert" = 'None' THEN NULL::DOUBLE PRECISION
    WHEN opp."auftragswert" ~ '^[0-9.-]+$' THEN opp."auftragswert"::DOUBLE PRECISION
    ELSE (
      REGEXP_REPLACE(
        REGEXP_REPLACE(opp."auftragswert", '[^0-9.,-]', '', 'g'),
        '(\d+)\.(\d{3})($|\D)', '\1\2', 'g'
      )
    )::DOUBLE PRECISION
  END AS "Amount",
  CASE 
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('chf') THEN 'CHF'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('eur', '€', 'euro') THEN 'EUR'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('usd', '$', 'dollar') THEN 'USD'
    WHEN LOWER(TRIM(opp."waehrungscode")) IN ('gbp', '£') THEN 'GBP'
    ELSE UPPER(TRIM(opp."waehrungscode"))
  END AS "CurrencyIsoCode",
  REPLACE(k."kundennummer", 'CUST-', 'ACCT-') AS "AccountId",
  opp."opp_kennung" AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} opp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
  ON REPLACE(opp."kunden_ref", 'KD-', 'CUST-') = k."kundennummer"