{{ config(materialized='table') }}

WITH opportunity_data AS (
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

account_mapping AS (
  SELECT
    kundennummer AS legacy_customer_id,
    SUBSTRING(MD5('Account_' || kundennummer) FROM 1 FOR 18) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

-- Parse dates from various formats
parsed_dates AS (
  SELECT
    opp_kennung,
    titel,
    vertriebsphase,
    waehrungscode,
    kunden_ref,
    -- Parse auftragswert: handle currency prefixes and various formats
    CASE 
      WHEN auftragswert IS NULL OR TRIM(auftragswert) IN ('', 'None', 'null') THEN NULL
      WHEN auftragswert ~ '^[0-9\-]+\.?[0-9]*$' THEN auftragswert::DOUBLE PRECISION
      WHEN auftragswert ~ '^[0-9\-]+,[0-9]+$' THEN 
        -- European format: 1.234,56 -> 1234.56
        REGEXP_REPLACE(auftragswert, '[.]', '', 'g')::DOUBLE PRECISION / 
        POWER(10, LENGTH(SPLIT_PART(auftragswert, ',', 2)))
      ELSE 
        -- Try to extract numeric value from strings like "EUR 144893.69"
        CASE 
          WHEN auftragswert ~ '([0-9]+[.,]?[0-9]*)' THEN 
            REGEXP_REPLACE(
              REGEXP_REPLACE(auftragswert, '[^0-9.,\-]', '', 'g'),
              '^([0-9]+)\.([0-9]+),([0-9]+)$', '\1\2.\3',
              'g'
            )::DOUBLE PRECISION
          ELSE NULL
        END
    END AS amount_value,
    
    -- Parse zieldatum from various formats
    CASE 
      WHEN zieldatum IS NULL OR TRIM(zieldatum) IN ('', 'N/A', '0000-00-00') THEN NULL
      WHEN zieldatum ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
      WHEN zieldatum ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN zieldatum ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN zieldatum ~ '^[0-9]{8}$' THEN 
        TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN zieldatum ~ '^[0-9]{4}[0-9]{2}[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
      ELSE NULL
    END AS close_date
  FROM opportunity_data
)

SELECT
  -- Generate deterministic Salesforce-style Id
  SUBSTRING(MD5('Opportunity_' || p.opp_kennung) FROM 1 FOR 18) AS "Id",
  
  -- Name: use titel
  COALESCE(NULLIF(TRIM(p.titel), ''), p.opp_kennung) AS "Name",
  
  -- StageName: normalize to enum values
  CASE 
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION', 'IN PRÜFUNG') THEN 'Qualification'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDER') THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('CLOSED WON', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'WON') THEN 'Closed Won'
    WHEN UPPER(TRIM(p.vertriebsphase)) IN ('CLOSED LOST', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN', 'LOST') THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  
  -- CloseDate: required, use parsed date or current date as fallback
  COALESCE(p.close_date, TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD')) AS "CloseDate",
  
  -- Amount
  p.amount_value AS "Amount",
  
  -- CurrencyIsoCode
  CASE 
    WHEN UPPER(TRIM(p.waehrungscode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
    WHEN UPPER(TRIM(p.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
    WHEN UPPER(TRIM(p.waehrungscode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
    WHEN UPPER(TRIM(p.waehrungscode)) IN ('GBP', 'POUND') THEN 'GBP'
    ELSE UPPER(TRIM(p.waehrungscode))
  END AS "CurrencyIsoCode",
  
  -- AccountId: lookup from master_kunden via kunden_ref (KD-MXXXX -> CUST-MXXXX)
  am.account_id AS "AccountId",
  
  -- Legacy Opportunity ID
  p.opp_kennung AS "Legacy_Opportunity_ID__c",
  
  -- CreatedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  
  -- LastModifiedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- IsDeleted: default to 0
  0 AS "IsDeleted"

FROM parsed_dates p
LEFT JOIN account_mapping am ON REPLACE(p.kunden_ref, 'KD-', 'CUST-') = am.legacy_customer_id
