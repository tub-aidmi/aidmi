{{
  config(
    materialized='table'
  )
}}

SELECT
  MD5(master_opportunities.opp_kennung) AS "Id",
  COALESCE(master_opportunities.titel, 'Unknown Opportunity') AS "Name",
  CASE
    WHEN LOWER(master_opportunities.vertriebsphase) = 'prospecting' THEN 'Prospecting'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'qualification' THEN 'Qualification'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'needs analysis' THEN 'Needs Analysis'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'value proposition' THEN 'Value Proposition'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'id. decision makers' THEN 'Id. Decision Makers'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'perception analysis' THEN 'Perception Analysis'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'negotiation/review' THEN 'Negotiation/Review'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'closed won' THEN 'Closed Won'
    WHEN LOWER(master_opportunities.vertriebsphase) = 'closed lost' THEN 'Closed Lost'
    ELSE 'Prospecting' -- Default for NOT NULL StageName
  END AS "StageName",
  COALESCE(
    TO_CHAR(TO_DATE(master_opportunities.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_opportunities.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_opportunities.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL CloseDate
  ) AS "CloseDate",
  CASE
    WHEN master_opportunities.auftragswert IS NULL OR TRIM(master_opportunities.auftragswert) = '' THEN NULL
    ELSE
      CASE
        -- European format (e.g., 1.234,56): remove dots, replace comma with dot
        WHEN master_opportunities.auftragswert ~ '^[\s€$£]*\d{1,3}(\.\d{3})*,\d{1,2}[\s€$£]*$' THEN
          REPLACE(REPLACE(
              REGEXP_REPLACE(master_opportunities.auftragswert, '[^0-9.,]+', '', 'g'),
              '.', ''
          ), ',', '.')::DOUBLE PRECISION
        -- US/Standard format (e.g., 1,234.56 or 1234.56): remove commas, then cast
        WHEN master_opportunities.auftragswert ~ '^[\s€$£]*\d{1,3}(,\d{3})*\.\d{1,2}[\s€$£]*$' THEN
          REPLACE(
              REGEXP_REPLACE(master_opportunities.auftragswert, '[^0-9.,]+', '', 'g'),
              ',', ''
          )::DOUBLE PRECISION
        -- Handle simple numbers potentially with a comma as decimal (e.g., 1234,56) or dot as decimal (1234.56)
        WHEN master_opportunities.auftragswert ~ '^[\s€$£]*\d+(?:[.,]\d{1,2})?[\s€$£]*$' THEN
          REPLACE(
              REGEXP_REPLACE(master_opportunities.auftragswert, '[^0-9.,]+', '', 'g'),
              ',', '.' -- Assume comma is decimal if no thousands dot was found
          )::DOUBLE PRECISION
        ELSE NULL
      END
  END AS "Amount",
  master_opportunities.waehrungscode AS "CurrencyIsoCode",
  MD5(master_opportunities.kunden_ref) AS "AccountId", -- Assuming kunden_ref links to master_kunden.kundennummer
  master_opportunities.opp_kennung AS "Legacy_Opportunity_ID__c",
  NOW()::TEXT AS "CreatedDate",
  NOW()::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_opportunities') }} AS master_opportunities
