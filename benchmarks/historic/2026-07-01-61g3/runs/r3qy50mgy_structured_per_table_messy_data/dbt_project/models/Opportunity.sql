{{ config(materialized='table') }}

WITH raw_opportunity AS (
    SELECT
        "Id",
        "Name",
        "StageName",
        "CloseDate",
        "Amount",
        "CurrencyIsoCode",
        "AccountId"
    FROM {{ source('fixture_messy_data_src', 'Opportunity') }}
),

-- Stage name normalization
stage_mapped AS (
    SELECT
        *,
        CASE LOWER(TRIM("StageName"))
            WHEN 'closed won'  THEN 'Closed Won'
            WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
            WHEN 'gewonnen'   THEN 'Closed Won'
            WHEN 'won'        THEN 'Closed Won'
            WHEN 'closed lost'  THEN 'Closed Lost'
            WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
            WHEN 'verloren'   THEN 'Closed Lost'
            WHEN 'lost'       THEN 'Closed Lost'
            WHEN 'prospecting'  THEN 'Prospecting'
            WHEN 'prospect'     THEN 'Prospecting'
            WHEN 'qualification' THEN 'Qualification'
            WHEN 'quali'         THEN 'Qualification'
            WHEN 'qualifikation' THEN 'Qualification'
            ELSE NULL
        END AS stage_name_cleaned,
        
        -- Amount cleanup: strip currency symbols/words, handle European format
        CASE 
            WHEN TRIM("Amount") IS NULL OR LOWER(TRIM("Amount")) = 'none' THEN NULL
            ELSE
                CAST(
                    CASE 
                        -- European format with thousands dots and decimal comma (e.g. 404.415,29)
                        WHEN REGEXP_REPLACE(TRIM("Amount"), '(?i)[€$£¥]|\s*(?:EUR|USD|GBP|CHF|DOLLAR)\b', '') ~ '^\-?[0-9]+(\.[0-9]+)*,[0-9]+$'
                            THEN REGEXP_REPLACE(
                                REGEXP_REPLACE(TRIM("Amount"), '(?i)[€$£¥]|\s*(?:EUR|USD|GBP|CHF|DOLLAR)\b', ''),
                                '\\.', ''
                              )  -- remove thousand-separator dots
                            || '.'  -- placeholder, will replace comma below
                    END
                ) AS DOUBLE PRECISION)
        END AS amount_raw,

        -- CloseDate parsing helper - returns formatted ISO date or NULL
        CASE 
            WHEN TRIM("CloseDate") IS NULL OR TRIM(LOWER(TRIM("CloseDate"))) IN ('n/a', '0000-00-00') THEN NULL
            WHEN TRIM("CloseDate") ~ '^\d{8}$'  -- YYYYMMDD
                THEN TO_DATE(TRIM("CloseDate"), 'YYYYMMDD')::TEXT
            WHEN TRIM("CloseDate") ~ '^\d{4}-\d{2}-\d{2}$'  -- YYYY-MM-DD (already ISO)
                THEN TRIM("CloseDate")
            WHEN TRIM("CloseDate") ~ '^\d{2}\.\d{2}\.\d{4}$'  -- DD.MM.YYYY
                THEN TO_DATE(TRIM("CloseDate"), 'DD.MM.YYYY')::TEXT
            WHEN TRIM("CloseDate") ~ '^\d+/\d+/\d{4}$'  -- MM/DD/YYYY (e.g. 11/26/2025 proves US format)
                THEN TO_DATE(TRIM("CloseDate"), 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS close_date_iso
        
    FROM raw_opportunity
),

amount_final AS (
    SELECT
        *,
        -- Second pass for amounts: handle the European comma-to-dot conversion properly
        CASE 
            WHEN amount_raw IS NOT NULL AND amount_raw ~ '[,]' THEN
                CAST(REPLACE(amount_raw::TEXT, ',', '.') AS DOUBLE PRECISION)
            ELSE amount_raw
        END AS amount_cleaned
    FROM stage_mapped
)

SELECT
    "Id" AS "Id",
    TRIM("Name") AS "Name",
    stage_name_cleaned AS "StageName",
    close_date_iso AS "CloseDate",
    COALESCE(amount_cleaned, 0.0) AS "Amount",
    -- Normalize currency code: map symbols and case variants to standard ISO codes
    CASE UPPER(TRIM(COALESCE("CurrencyIsoCode", '')))
        WHEN 'CHF' THEN 'CHF'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'USD' THEN 'USD'
        WHEN 'DOLLAR' THEN NULL
        WHEN '' THEN NULL
        ELSE UPPER(TRIM("CurrencyIsoCode"))
    END AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    -- Legacy fields with no source mapping — set to NULL
    NULL AS "Legacy_Opportunity_ID__c",
    -- Audit fields not present in source
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM amount_final