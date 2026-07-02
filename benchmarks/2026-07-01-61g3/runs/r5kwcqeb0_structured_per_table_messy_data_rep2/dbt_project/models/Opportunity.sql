{{ config(materialized='table') }}

SELECT
    Id AS "Id",
    COALESCE(INITCAP(TRIM(Name)), 'Unnamed Opportunity') AS "Name",
    COALESCE(
        CASE LOWER(TRIM(StageName))
            WHEN 'lost' THEN 'Closed Lost'
            WHEN 'verloren' THEN 'Closed Lost'
            WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
            WHEN 'won' THEN 'Closed Won'
            WHEN 'gewonnen' THEN 'Closed Won'
            WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
            WHEN 'closed won' THEN 'Closed Won'
            WHEN 'prospect' THEN 'Prospecting'
            WHEN 'prospecting' THEN 'Prospecting'
            WHEN 'quali' THEN 'Qualification'
            WHEN 'qualification' THEN 'Qualification'
            WHEN 'qualifikation' THEN 'Qualification'
            WHEN 'in prüfung' THEN 'Qualification'
            WHEN 'in kontakt' THEN 'Prospecting'
        END,
        'Qualification'  -- fallback for unmapped/uncleaned values
    ) AS "StageName",
    CASE
        WHEN CloseDate IS NULL OR TRIM(CloseDate) = ''
            THEN NULL
        WHEN TRIM(CloseDate) = 'N/A'
            THEN NULL
        WHEN CAST(TRIM(CloseDate) AS TEXT) ~ '^0000(-00-00)?$'
            THEN NULL
        -- ISO 8601: YYYY-MM-DD — already correct format
        WHEN TRIM(CloseDate) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TRIM(CloseDate)
        -- Compact: YYYYMMDD
        WHEN TRIM(CloseDate) ~ '^\d{8}$'
            THEN SUBSTR(TRIM(CloseDate), 1, 4) || '-'
               || SUBSTR(TRIM(CloseDate), 5, 2) || '-'
               || SUBSTR(TRIM(CloseDate), 7, 2)
        -- US format: MM/DD/YYYY
        WHEN TRIM(CloseDate) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(CloseDate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        -- German/European format: DD.MM.YYYY
        WHEN TRIM(CloseDate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(CloseDate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(Amount) IS NULL OR TRIM(Amount) = ''
            THEN NULL
        WHEN UPPER(TRIM(Amount)) = 'NONE'
            THEN NULL
        -- European format with dot as thousands sep and comma as decimal: e.g. "404.415,29"
        WHEN TRIM(Amount) ~ '^-?\d{1,3}(\.\d{3})+,\d{2}$'
            THEN REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(Amount), '[€$£\s]', '', 'g'), '\.', '', 'g'),
                ',', '.', ''
                )::DOUBLE PRECISION
        -- Standard decimal: e.g. "426328.65" or "-178436.34" or "0"
        WHEN TRIM(Amount) ~ '^-?\d+(\.\d+)?$'
            THEN CAST(TRIM(Amount) AS DOUBLE PRECISION)
        -- Amount with currency prefix like "EUR 426328.65" or "USD 1042.7"
        WHEN TRIM(Amount) ~ '^[A-Z]{3}\s+-?\d+(\.\d+)?$'
            THEN CAST(REGEXP_REPLACE(TRIM(Amount), '^[A-Z]{3}\s+', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE LOWER(TRIM(CurrencyIsoCode))
        WHEN 'eur' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'usd' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN 'gbp' THEN 'GBP'
        WHEN 'chf' THEN 'CHF'
        ELSE UPPER(TRIM(CurrencyIsoCode))
    END AS "CurrencyIsoCode",
    AccountId AS "AccountId",
    Id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}