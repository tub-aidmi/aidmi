{{ config(materialized='table') }}

SELECT
  TRIM(mo.opp_kennung) AS "Id",
  COALESCE(TRIM(mo.titel), 'Unnamed Opportunity') AS "Name",
  CASE
    WHEN TRIM(mo.vertriebsphase) = 'Lead' THEN 'Prospecting'
    WHEN TRIM(mo.vertriebsphase) = 'Qualifizierung' THEN 'Qualification'
    WHEN TRIM(mo.vertriebsphase) = 'Bedarfsanalyse' THEN 'Needs Analysis'
    WHEN TRIM(mo.vertriebsphase) = 'Wertangebot' THEN 'Value Proposition'
    WHEN TRIM(mo.vertriebsphase) = 'Entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
    WHEN TRIM(mo.vertriebsphase) = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
    WHEN TRIM(mo.vertriebsphase) = 'Angebot/Preis' THEN 'Proposal/Price Quote'
    WHEN TRIM(mo.vertriebsphase) = 'Verhandlung/Prüfung' THEN 'Negotiation/Review'
    WHEN TRIM(mo.vertriebsphase) = 'Gewonnen' THEN 'Closed Won'
    WHEN TRIM(mo.vertriebsphase) = 'Verloren' THEN 'Closed Lost'
    ELSE 'Prospecting' -- Default for NOT NULL constraint if no match or NULL
  END AS "StageName",
  COALESCE(
    CASE
      WHEN TRIM(mo.zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(mo.zieldatum) -- YYYY-MM-DD
      WHEN TRIM(mo.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(mo.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN TRIM(mo.zieldatum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(mo.zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN TRIM(mo.zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(mo.zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE '1900-01-01' -- Default for unparseable or NULL
    END,
    '1900-01-01' -- Final COALESCE to ensure NOT NULL
  ) AS "CloseDate",
  CASE
    WHEN TRIM(mo.auftragswert) IS NULL OR TRIM(mo.auftragswert) = '' THEN NULL
    WHEN TRIM(mo.auftragswert) ~ '^-?\d+\.\d{3}(,\d+)?$' THEN -- e.g. 1.234,56 or 1.234.567,89 (European with thousand sep)
        REPLACE(REPLACE(TRIM(mo.auftragswert), '.', ''), ',', '.')::DOUBLE PRECISION
    WHEN TRIM(mo.auftragswert) ~ '^-?\d+,\d+$' THEN -- e.g. 1234,56 (European without thousand sep)
        REPLACE(TRIM(mo.auftragswert), ',', '.')::DOUBLE PRECISION
    WHEN TRIM(mo.auftragswert) ~ '^-?\d+(\.\d+)?$' THEN -- e.g. 1234.56 or 1234 (US format)
        TRIM(mo.auftragswert)::DOUBLE PRECISION
    ELSE NULL
  END AS "Amount",
  TRIM(mo.waehrungscode) AS "CurrencyIsoCode",
  TRIM(mo.kunden_ref) AS "AccountId",
  TRIM(mo.opp_kennung) AS "Legacy_Opportunity_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_opportunities') }} AS mo
