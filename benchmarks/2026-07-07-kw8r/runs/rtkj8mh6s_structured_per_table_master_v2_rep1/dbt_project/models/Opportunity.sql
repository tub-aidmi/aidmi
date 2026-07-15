{{ config(materialized='table') }}

SELECT
  'OPP-' || TRIM(o.opp_kennung) AS "Id",
  INITCAP(TRIM(o.titel)) AS "Name",
  CASE
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('akquise', 'neukundengewinnung', 'prospecting') THEN 'Prospecting'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualifikation', 'qualification', 'lead qualifizierung') THEN 'Qualification'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('bedarfsanalyse', 'needs analysis', 'bedarfsermittlung') THEN 'Needs Analysis'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('wertversprechen', 'value proposition', 'konzeptentwicklung') THEN 'Value Proposition'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('entscheider identifizieren', 'decision maker identification', 'id. decision makers') THEN 'Id. Decision Makers'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis', 'meinungsanalyse') THEN 'Perception Analysis'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('angebot/price quote', 'angebotserstellung', 'proposal/price quote') THEN 'Proposal/Price Quote'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('verhandlung', 'negotiation/review', 'verhandlungsphase', 'negotiation') THEN 'Negotiation/Review'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('gewonnen', 'closed won', 'auftrag erteilt', 'bezahlung erhalten') THEN 'Closed Won'
    WHEN LOWER(TRIM(o.vertriebsphase)) IN ('verloren', 'closed lost', 'auftrag abgelehnt', 'nicht gewonnen') THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  CASE
    WHEN o.zieldatum IS NOT NULL AND TRIM(o.zieldatum) != ''
      AND o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
      TO_DATE(TRIM(o.zieldatum), 'DD.MM.YYYY')::TEXT
    WHEN o.zieldatum IS NOT NULL AND TRIM(o.zieldatum) != ''
      AND o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
      TRIM(o.zieldatum)
    ELSE NULL
  END AS "CloseDate",
  CASE
    WHEN o.auftragswert IS NOT NULL AND TRIM(o.auftragswert) != '' THEN
      CASE
        WHEN REGEXP_REPLACE(TRIM(o.auftragswert), '[^\d.,-]', '', 'g') ~ '\d' THEN
          CAST(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(o.auftragswert), '[^\d.,-]', '', 'g'),
               '\.', '', 'g'),
              ',', '.', 'g') AS DOUBLE PRECISION)
        ELSE NULL
      END
    ELSE NULL
  END AS "Amount",
  CASE WHEN waehrungscode IS NOT NULL AND TRIM(waehrungscode) != '' THEN UPPER(TRIM(waehrungscode)) ELSE NULL END AS "CurrencyIsoCode",
  CASE
    WHEN c.kundennummer IS NOT NULL AND TRIM(c.kundennummer) != '' THEN 'ACCT-' || TRIM(c.kundennummer)
    ELSE NULL
  END AS "AccountId",
  TRIM(o.opp_kennung) AS "Legacy_Opportunity_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
  ON TRIM(o.kunden_ref) = c.kundennummer
WHERE o.opp_kennung IS NOT NULL
  AND TRIM(o.opp_kennung) != ''