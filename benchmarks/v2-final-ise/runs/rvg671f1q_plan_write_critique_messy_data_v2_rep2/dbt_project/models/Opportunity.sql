{{ config(materialized='table') }}

WITH source_opportunity AS (
  SELECT * FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
source_account AS (
  SELECT id, name FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT
  TRIM(UPPER(o.id)) AS "Id",
  COALESCE(TRIM(INITCAP(o.name)), 'Untitled') AS "Name",
  CASE UPPER(TRIM(COALESCE(o.stagename, '')))
    WHEN 'PROSPECTING' THEN 'Prospecting'
    WHEN 'QUALIFICATION' THEN 'Qualification'
    WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
    WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
    WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
    WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
    WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
    WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
    WHEN 'CLOSED WON' THEN 'Closed Won'
    WHEN 'CLOSED LOST' THEN 'Closed Lost'
    ELSE NULL
  END AS "StageName",
  CASE 
    WHEN TRIM(COALESCE(o.closedate, '')) = '' THEN NULL
    WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY')::TEXT
    WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(o.closedate), 'YYYY-MM-DD')::TEXT
    WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY')::TEXT
    WHEN o.closedate ~ '^\d{8}$' THEN TO_DATE(TRIM(o.closedate), 'YYYYMMDD')::TEXT
    ELSE NULL
  END AS "CloseDate",
  CASE 
    WHEN TRIM(COALESCE(o.amount, '')) = '' THEN NULL
    ELSE CAST(
      -- Step 1: strip all non-numeric characters except dots and commas
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          TRIM(o.amount), '[A-Za-z]', '', 'g'  -- remove currency codes like UR, USD, EUR
        ),
        '[^0-9.,\-]', '', 'g'                   -- remove anything else except numbers, dots, commas, minus
      )

      -- Step 2: handle locale separators
      CASE
        -- Both dot and comma present (mixed locale format)
        WHEN REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%.%' 
             AND REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%,%'
        THEN
          CASE
            -- European: last separator is comma (e.g. 1.234,56 or 1.234.567,89)
            WHEN POSITION(',' IN REGEXP_REPLACE(
                   TRIM(o.amount), '[A-Za-z]', '', 'g'
                 )) 
                  > POSITION('.' IN REGEXP_REPLACE(
                   TRIM(o.amount), '[A-Za-z]', '', 'g'
                 ))
            THEN REGEXP_REPLACE(
                   REGEXP_REPLACE(
                     REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g'), 
                      '\.', '', 'g'),   -- remove thousand-sep dots
                  ',','.')      -- replace decimal comma with dot

            -- US format: last separator is dot (e.g. 1,234.56)
            ELSE REGEXP_REPLACE(
                   REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g'), 
                    ',', '')            -- remove thousand-sep commas
          END

        -- Only commas present, no dots: check if decimal or thousand separators
        WHEN REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%,%' 
             AND NOT REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%.%'
        THEN
          CASE
            -- Single comma with digits after (like "1234,") → decimal separator
            WHEN REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g') ~ ',[0-9]+$' 
                 AND NOT REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g') ~ ',.*,'
            THEN REGEXP_REPLACE(
                   REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g'), ',', '.')
             -- Multiple commas or comma not at end → thousand separators
            ELSE REGEXP_REPLACE(
                   REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g'), ',', '')
          END

        -- Only dots present, no commas: treat as thousand separators
        WHEN REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%.%' 
             AND NOT REGEXP_REPLACE(
               TRIM(o.amount), '[A-Za-z]', '', 'g'
             ) LIKE '%,%'
        THEN REGEXP_REPLACE(
               REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g'), '\.', '', 'g')

        -- Plain number (no dots or commas) - already clean
        ELSE REGEXP_REPLACE(TRIM(o.amount), '[A-Za-z]', '', 'g')
      END
    AS DOUBLE PRECISION)
  END AS "Amount",
  UPPER(TRIM(COALESCE(o.currencyisocode, ''))) AS "CurrencyIsoCode",
  TRIM(UPPER(a.id)) AS "AccountId",
  TRIM(UPPER(o.id)) AS "Legacy_Opportunity_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0::INTEGER AS "IsDeleted"
FROM source_opportunity o
LEFT JOIN source_account a
  ON TRIM(UPPER(o.accountid)) = TRIM(UPPER(a.id))