{{ config(materialized='table') }}

WITH opp AS (
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
      -- Additional standard pipeline stages (literal matches)
      ELSE NULL
    END AS "StageName",

    -- CloseDate: parse multiple formats, fallback to 1900-01-01 for NOT NULL
    CASE
      WHEN closedate IS NULL OR TRIM(closedate) = '' THEN '1900-01-01'
      WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
      WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
      WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
      WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
      ELSE '1900-01-01'
    END AS "CloseDate",

    -- Amount: strip currency text/prefixes, handle European format (dot=thousands, comma=decimal)
    CASE
      WHEN LOWER(TRIM(amount)) IN ('none', '', 'null', '-') THEN NULL
      ELSE
        CAST(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                TRIM(amount),
                '^(USD|EUR|GBP|CHF)\s*', '', 'i'       -- strip currency prefix
              ),
              '\$', ''                                  -- strip dollar sign
            ),
            '(?<!\d)\.(?=\d{3}(?:[.,]|$))', ''          -- remove thousand-separator dots (digits followed by 3+ digits then dot/comma/end)
          )
        ::DOUBLE PRECISION
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
      ELSE TRIM(currencyisocode)  -- pass through as-is for unknown codes
    END AS "CurrencyIsoCode",

    -- AccountId: transform CUST-XXXX → 001XXXX to reference Salesforce Account Id
    CASE
      WHEN accountid IS NOT NULL AND accountid ~ '^CUST-' THEN '001' || SUBSTRING(accountid FROM 6)
      ELSE accountid
    END AS "AccountId",

    -- Legacy key: preserve original source id
    id AS "Legacy_Opportunity_ID__c",

    -- Audit columns (not in source)
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    NULL::INTEGER AS "IsDeleted"

  FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
-- Ensure NOT NULL StageName constraint: fallback unmapped stages to Prospecting
opp_final AS (
  SELECT
    "Id",
    "Name",
    COALESCE("StageName", 'Prospecting') AS "StageName",
    "CloseDate",
    CAST("Amount" AS DOUBLE PRECISION) AS "Amount",
    "CurrencyIsoCode",
    "AccountId",
    "Legacy_Opportunity_ID__c",
    "CreatedDate",
    "LastModifiedDate",
    "IsDeleted"
  FROM opp
)

SELECT * FROM opp_final