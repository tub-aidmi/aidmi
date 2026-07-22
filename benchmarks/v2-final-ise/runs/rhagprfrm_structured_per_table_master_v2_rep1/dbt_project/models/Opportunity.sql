{{ config(materialized='table') }}

WITH customers AS (
  SELECT
    TRIM(kundennummer) AS kundennummer,
    LOWER(TRIM(undernehmensname)) AS unternehmensname_normalized
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
opportunities AS (
  SELECT
    opp_kennung,
    titel,
    vertriebsphase,
    zieldatum,
    auftragswert,
    waehrungscode,
    kunden_ref
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
  -- Id: Salesforce-style opportunity ID
  'OPP-' || TRIM(o.opp_kennung) AS "Id",
  
  -- Name: opportunity title
  INITCAP(TRIM(o.titel)) AS "Name",
  
  -- StageName: map German sales phases to English Salesforce pipeline stages
  CASE
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('akquise', 'neukundengewinnung', 'prospecting') THEN 'Prospecting'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualifikation', 'qualification', 'lead qualifizierung') THEN 'Qualification'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('bedarfsanalyse', 'needs analysis', 'bedarfsermittlung') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('wertversprechen', 'value proposition', 'konzeptentwicklung') THEN 'Value Proposition'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('entscheider identifizieren', 'decision maker identification', 'id. decision makers') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis', 'meinungsanalyse') THEN 'Perception Analysis'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('angebot/price quote', 'angebotserstellung', 'proposal/price quote', 'angebotserstellung') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('verhandlung', 'negotiation/review', 'verhandlungsphase', 'negotiation') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('gewonnen', 'closed won', 'auftrag erteilt', 'bezahlung erhalten') THEN 'Closed Won'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('verloren', 'closed lost', 'auftrag abgelehnt', 'nicht gewonnen') THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  
  -- CloseDate: parse DD.MM.YYYY format from German source, output ISO YYYY-MM-DD
  CASE
    WHEN o.zieldatum IS NOT NULL AND TRIM(o.zieldatum) != '' 
      AND o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
      TO_DATE(TRIM(o.zieldatum), 'DD.MM.YYYY')::TEXT
    WHEN o.zieldatum IS NOT NULL AND TRIM(o.zieldatum) != ''
      AND o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
      TRIM(o.zieldatum)
    ELSE NULL
  END AS "CloseDate",
  
  -- Amount: strip currency symbols, handle European number format (dots=thousands, comma=decimal)
  CASE
    WHEN o.auftragswert IS NOT NULL AND TRIM(o.auftragswert) != '' THEN
      CAST(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(o.auftragswert), '[^\d.,-]', ''), -- keep digits, dot, comma, minus
            '\.', ''),                                          -- remove thousand-sep dots
          ',', '.')                                            -- replace decimal comma with period
      AS DOUBLE PRECISION)
    ELSE NULL
  END AS "Amount",
  
  -- CurrencyIsoCode: uppercase and trim the currency code
  UPPER(TRIM(o.waehrungscode)) AS "CurrencyIsoCode",
  
  -- AccountId: join to customers table, generate Salesforce-style Account ID
  CASE 
    WHEN c.kundennummer IS NOT NULL THEN 'ACCT-' || c.kundennummer
    ELSE NULL
  END AS "AccountId",
  
  -- Legacy_Opportunity_ID__c: raw source key for traceability
  TRIM(o.opp_kennung) AS "Legacy_Opportunity_ID__c",
  
  -- Standard Audit fields (source doesn't provide these, use NULL or generated values)
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM opportunities o
LEFT JOIN customers c
  ON TRIM(o.kunden_ref) = c.kundennummer
WHERE o.opp_kennung IS NOT NULL
  AND TRIM(o.opp_kennung) != ''