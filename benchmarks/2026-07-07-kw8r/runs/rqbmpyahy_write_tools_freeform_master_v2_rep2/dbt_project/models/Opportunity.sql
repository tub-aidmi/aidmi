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
    kundennummer,
    '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
  -- Generate deterministic Salesforce-style ID from natural key
  '006' || SUBSTRING(MD5(o.opp_kennung) FROM 1 FOR 15) AS "Id",
  
  -- Name (required)
  COALESCE(NULLIF(TRIM(o.titel), ''), 'Opportunity ' || o.opp_kennung) AS "Name",
  
  -- StageName: map to enum values
  CASE 
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('QUALIFICATION', 'QUALI') THEN 'Qualification'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN') THEN 'Id. Decision Makers'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT') THEN 'Proposal/Price Quote'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG') THEN 'Negotiation/Review'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'CLOSED WON') THEN 'Closed Won'
    WHEN UPPER(TRIM(o.vertriebsphase)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",
  
  -- CloseDate: parse various date formats
  CASE 
    WHEN o.zieldatum IS NULL OR o.zieldatum IN ('N/A', 'None', '') THEN NULL
    WHEN o.zieldatum = '0000-00-00' THEN NULL
    WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN 
      CASE 
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' AND 
             CAST(SUBSTRING(o.zieldatum FROM 6 FOR 2) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SUBSTRING(o.zieldatum FROM 9 FOR 2) AS INTEGER) BETWEEN 1 AND 31
        THEN o.zieldatum
        ELSE NULL
      END
    WHEN o.zieldatum ~ '^\d{8}$' THEN 
      TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN o.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
      TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN o.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{2}$' THEN 
      TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YY'), 'YYYY-MM-DD')
    WHEN o.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN 
      TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "CloseDate",
  
  -- Amount: parse various formats
  CASE 
    WHEN o.auftragswert IS NULL OR o.auftragswert IN ('None', 'N/A', '') THEN NULL
    WHEN o.auftragswert ~ '^[0-9.-]+$' THEN 
      CASE 
        WHEN o.auftragswert ~ '^\d+\.\d+,\d+$' THEN 
          -- European format: 1.234,56 -> 1234.56
          CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9.-]+$' THEN 
          CAST(o.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
      END
    WHEN o.auftragswert ~ '^[A-Za-z]+ [0-9.,-]+$' THEN 
      -- Currency prefix like "EUR 100.00" or "EUR 100,00"
      CASE 
        WHEN o.auftragswert ~ '[0-9]+\.[0-9]+,[0-9]+' THEN 
          CAST(REPLACE(REPLACE(REGEXP_REPLACE(o.auftragswert, '^[A-Za-z]+ ', ''), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE 
          CAST(REGEXP_REPLACE(o.auftragswert, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
      END
    ELSE NULL
  END AS "Amount",
  
  -- CurrencyIsoCode
  CASE 
    WHEN UPPER(TRIM(o.waehrungscode)) IN ('EUR', 'EURO') THEN 'EUR'
    WHEN UPPER(TRIM(o.waehrungscode)) IN ('CHF', 'SCHWEIZER FRANKEN') THEN 'CHF'
    WHEN UPPER(TRIM(o.waehrungscode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
    WHEN UPPER(TRIM(o.waehrungscode)) IN ('GBP', 'BRITISH POUND') THEN 'GBP'
    WHEN o.waehrungscode IN ('€') THEN 'EUR'
    ELSE UPPER(TRIM(o.waehrungscode))
  END AS "CurrencyIsoCode",
  
  -- AccountId: join to customer using kunden_ref (KD-M* -> CUST-M*)
  am.account_id AS "AccountId",
  
  -- Legacy Opportunity ID from source natural key
  o.opp_kennung AS "Legacy_Opportunity_ID__c",
  
  -- Timestamps
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- Not deleted
  0 AS "IsDeleted"

FROM opportunity_data o
LEFT JOIN account_mapping am ON REPLACE(o.kunden_ref, 'KD-M', 'CUST-M') = am.kundennummer
