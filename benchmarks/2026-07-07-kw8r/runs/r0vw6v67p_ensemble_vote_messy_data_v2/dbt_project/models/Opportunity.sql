{{ config(materialized='table') }}

SELECT
  -- Id: Salesforce-style (006 prefix replaces OPP-)
  '006' || SUBSTRING(id FROM 5) AS "Id",

  -- Name: NOT NULL, default to placeholder if empty
  COALESCE(NULLIF(TRIM(name), ''), 'Unnamed Opportunity') AS "Name",

  -- StageName mapping (all observed variants)
  CASE LOWER(TRIM(stagename))
    WHEN 'prospecting' THEN 'Prospecting'
    WHEN 'prospect'    THEN 'Prospecting'
    WHEN 'qualification' THEN 'Qualification'
    WHEN 'qualifikation' THEN 'Qualification'
    WHEN 'quali'       THEN 'Qualification'
    WHEN 'in prüfung'  THEN 'Needs Analysis'
    WHEN 'in pruefung' THEN 'Needs Analysis'
    WHEN 'in kontakt'  THEN 'Prospecting'
    WHEN 'gewonnen'    THEN 'Closed Won'
    WHEN 'closed won'  THEN 'Closed Won'
    WHEN 'won'         THEN 'Closed Won'
    WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
    WHEN 'lost'        THEN 'Closed Lost'
    WHEN 'closed lost' THEN 'Closed Lost'
    WHEN 'verloren'    THEN 'Closed Lost'
    WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
    ELSE 'Prospecting'
  END AS "StageName",

  -- CloseDate: parse multiple formats, fallback to NULL if unparseable
  CASE
    WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
    WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
    WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
    WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
    WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
    ELSE NULL
  END AS "CloseDate",

  -- Amount: strip currency text/symbols, handle European format (dots=thousands, comma=decimal)
  CASE
    WHEN LOWER(TRIM(amount)) IN ('none', '', 'null', '-', '0') THEN CAST(0.0 AS DOUBLE PRECISION)
    ELSE
      CAST(
        -- First normalize: if both dot and comma present and dots appear before comma (European), fix it
        CASE
          WHEN amount ~ '\.' AND amount ~ ',' AND position('.' IN amount) < position(',' IN amount)
            THEN REGEXP_REPLACE(
              REPLACE(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g'), '.', ''), ',', '.')
          ELSE
            -- Otherwise just strip non-numeric chars except dot, minus, comma
            REGEXP_REPLACE(amount, '[^\d.,-]', '', 'g')
        END
      AS DOUBLE PRECISION)
  END AS "Amount",

  -- CurrencyIsoCode: normalize to ISO 4217 codes
  CASE UPPER(TRIM(currencyisocode))
    WHEN 'USD' THEN 'USD'
    WHEN '$'   THEN 'USD'
    WHEN 'EURO' THEN 'EUR'
    WHEN '€'   THEN 'EUR'
    WHEN 'EUR' THEN 'EUR'
    WHEN 'GBP' THEN 'GBP'
    WHEN '£'   THEN 'GBP'
    WHEN 'CHF' THEN 'CHF'
    WHEN 'DOLLAR' THEN 'USD'
    ELSE NULL
  END AS "CurrencyIsoCode",

  -- AccountId: transform CUST-XXXX → 001XXXX to reference Salesforce Account Id
  CASE
    WHEN accountid IS NOT NULL AND accountid ~ '^CUST-' THEN '001' || SUBSTRING(accountid FROM 6)
    ELSE NULL
  END AS "AccountId",

  -- Legacy key: preserve original source id
  id AS "Legacy_Opportunity_ID__c",

  -- Audit columns (not in source)
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  NULL::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}