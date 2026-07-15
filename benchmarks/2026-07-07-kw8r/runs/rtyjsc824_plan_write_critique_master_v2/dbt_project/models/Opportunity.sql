{{ config(materialized='table') }}
WITH opportunity_data AS (
  SELECT 
    mo.opp_kennung,
    mo.titel,
    mo.vertriebsphase,
    mo.zieldatum,
    mo.auftragswert,
    mo.waehrungscode,
    mo.kunden_ref,
    mk.kundennummer
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
    ON REPLACE(mo.kunden_ref, 'KD-', 'CUST-') = mk.kundennummer
)
SELECT 
  MD5(od.opp_kennung) AS "Id",
  INITCAP(TRIM(od.titel)) AS "Name",
  CASE 
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('prospecting', 'in kontakt') THEN 'Prospecting'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('qualification', 'quali') THEN 'Qualification'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('value proposition', 'wertargumentation') THEN 'Value Proposition'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('id. decision makers', 'entscheider identifiziert') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('proposal/price quote', 'angebot') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('negotiation/review', 'verhandlung') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
    WHEN LOWER(TRIM(od.vertriebsphase)) IN ('closed lost', 'abgeschlossen (verloren)', 'lost') THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  CASE 
    WHEN od.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN od.zieldatum
    WHEN od.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(od.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN od.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(od.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN od.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(od.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "CloseDate",
  CASE 
    WHEN od.auftragswert IS NULL OR TRIM(od.auftragswert) = 'None' THEN NULL
    WHEN od.auftragswert ~ '^-' THEN NULL
    WHEN od.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN CAST(REPLACE(REPLACE(od.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
    WHEN od.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(od.auftragswert, ',', '.') AS DOUBLE PRECISION)
    WHEN od.auftragswert ~ '^[0-9]+\.[0-9]{2}$' THEN CAST(od.auftragswert AS DOUBLE PRECISION)
    WHEN od.auftragswert ~ '^[0-9]+$' THEN CAST(od.auftragswert AS DOUBLE PRECISION)
    WHEN REGEXP_REPLACE(od.auftragswert, '[^0-9.,-]', '', 'g') ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN CAST(REPLACE(REPLACE(REGEXP_REPLACE(od.auftragswert, '[^0-9.,-]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
    WHEN REGEXP_REPLACE(od.auftragswert, '[^0-9.,-]', '', 'g') ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(REGEXP_REPLACE(od.auftragswert, '[^0-9.,-]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
    ELSE NULL
  END AS "Amount",
  CASE 
    WHEN od.waehrungscode IS NULL OR TRIM(od.waehrungscode) = '' THEN NULL
    WHEN UPPER(TRIM(REGEXP_REPLACE(od.waehrungscode, '[^A-Za-z]', '', 'g'))) IN ('EURO', 'EUR', '€') THEN 'EUR'
    WHEN UPPER(TRIM(REGEXP_REPLACE(od.waehrungscode, '[^A-Za-z]', '', 'g'))) IN ('DOLLAR', 'USD', '$') THEN 'USD'
    WHEN UPPER(TRIM(REGEXP_REPLACE(od.waehrungscode, '[^A-Za-z]', '', 'g'))) IN ('CHF') THEN 'CHF'
    WHEN UPPER(TRIM(REGEXP_REPLACE(od.waehrungscode, '[^A-Za-z]', '', 'g'))) IN ('GBP', '£') THEN 'GBP'
    ELSE NULL
  END AS "CurrencyIsoCode",
  CASE WHEN od.kundennummer IS NOT NULL THEN MD5(od.kundennummer) ELSE NULL END AS "AccountId",
  od.opp_kennung AS "Legacy_Opportunity_ID__c",
  '2023-01-01T00:00:00Z' AS "CreatedDate",
  '2023-01-01T00:00:00Z' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM opportunity_data od