{{ config(materialized='table') }}

WITH opportunity_data AS (
  SELECT 
    '' || MD5(COALESCE(opp_kennung, '') || COALESCE(titel, '')) AS opportunity_id,
    
    -- Join to customer to get AccountId
    '' || MD5(COALESCE(kd.kundennummer, '') || COALESCE(kd.unternehmensname, '')) AS account_id,
    
    COALESCE(NULLIF(TRIM(titel), ''), 'Untitled Opportunity') AS opportunity_name,
    
    -- Map vertriebsphase to StageName
    CASE 
      WHEN UPPER(TRIM(vertriebsphase)) IN ('PROSPEKTING', 'PROSPEKT') THEN 'Prospecting'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('QUALIFIZIERUNG', 'QUALIFICATION', 'QUALIFIKATION') THEN 'Qualification'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS', 'BEDARF') THEN 'Needs Analysis'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('WERTVORSCHLAG', 'VALUE PROPOSITION', 'WERTVOR') THEN 'Value Proposition'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('ENTSCHEIDUNGSTRÄGER IDENTIFIZIEREN', 'ID. DECISION MAKERS', 'ENTSCHEIDUNGSTRAGER') THEN 'Id. Decision Makers'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('WAHRNEHMUNGSANALYSE', 'PERCEPTION ANALYSIS', 'WAHRNEHMUNG') THEN 'Perception Analysis'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('ANGEBOT/PREIS', 'PROPOSAL/PRICE QUOTE', 'ANGEBOT') THEN 'Proposal/Price Quote'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('VERHANDLUNG/PRÜFUNG', 'NEGOTIATION/REVIEW', 'VERHANDLUNG') THEN 'Negotiation/Review'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('GESCHLOSSEN GEWONNEN', 'CLOSED WON', 'GEWONNEN') THEN 'Closed Won'
      WHEN UPPER(TRIM(vertriebsphase)) IN ('GESCHLOSSEN VERLOREN', 'CLOSED LOST', 'VERLOREN') THEN 'Closed Lost'
      ELSE 'Prospecting'
    END AS stage_name,
    
    -- Parse zieldatum (target date) - handle various formats
    CASE 
      WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
      WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN TRIM(zieldatum) ~ '^\d{4}\d{2}\d{2}$' THEN 
        TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS close_date,
    
    -- Parse auftragswert (amount) - handle various currency formats
    CASE 
      WHEN TRIM(auftragswert) ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
        -- European format: 1.234,56 -> 1234.56
        CAST(REPLACE(REPLACE(auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
      WHEN TRIM(auftragswert) ~ '^[0-9]+,[0-9]{2}$' THEN 
        -- European format: 1234,56 -> 1234.56
        CAST(REPLACE(auftragswert, ',', '.') AS DOUBLE PRECISION)
      WHEN TRIM(auftragswert) ~ '^[0-9]+\.[0-9]{2}$' THEN 
        -- Standard format: 1234.56
        CAST(auftragswert AS DOUBLE PRECISION)
      WHEN TRIM(auftragswert) ~ '^[€\$£]?[\s]?[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
        CAST(REPLACE(REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9.,]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
      WHEN TRIM(auftragswert) ~ '^[€\$£]?[\s]?[0-9]+,[0-9]{2}$' THEN 
        CAST(REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9.,]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
      ELSE NULL
    END AS amount,
    
    UPPER(TRIM(waehrungscode)) AS currency_iso_code,
    TRIM(opp_kennung) AS legacy_opportunity_id,
    '2024-01-01' AS created_date,
    '2024-01-01' AS last_modified_date,
    0 AS is_deleted
    
  FROM {{ source(source_slug, 'master_opportunities') }} o
  LEFT JOIN {{ source(source_slug, 'master_kunden') }} kd 
    ON TRIM(o.kunden_ref) = TRIM(kd.kundennummer)
)

SELECT 
  opportunity_id AS "Id",
  opportunity_name AS "Name",
  stage_name AS "StageName",
  close_date AS "CloseDate",
  amount AS "Amount",
  currency_iso_code AS "CurrencyIsoCode",
  account_id AS "AccountId",
  legacy_opportunity_id AS "Legacy_Opportunity_ID__c",
  created_date AS "CreatedDate",
  last_modified_date AS "LastModifiedDate",
  is_deleted AS "IsDeleted"

FROM opportunity_data
