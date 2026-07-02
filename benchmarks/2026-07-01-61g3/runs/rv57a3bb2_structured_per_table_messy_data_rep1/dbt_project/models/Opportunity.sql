{{ config(materialized='table') }}

SELECT
    Id,
    COALESCE(NULLIF(TRIM(Name), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(StageName)) IN ('prospect', 'prospecti', 'prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(StageName)) IN ('quali', 'qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(StageName)) IN ('in prüfung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(StageName)) IN ('value proposition', 'wertversprechen') THEN 'Value Proposition'
        WHEN LOWER(TRIM(StageName)) IN ('id. decision makers', 'identifizierung entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(StageName)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(StageName)) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(StageName)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(StageName)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closedwin') THEN 'Closed Won'
        WHEN LOWER(TRIM(StageName)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN CloseDate IS NULL OR TRIM(CloseDate) = '' OR TRIM(CloseDate) = 'N/A' OR TRIM(CloseDate) = '0000-00-00' THEN NULL
        -- YYYY-MM-DD format (ISO, already validated by regex to not be 0000-00-00)
        WHEN CloseDate ~ '^\d{4}-\d{2}-\d{2}$' AND CloseDate != '0000-00-00' THEN TO_DATE(CloseDate, 'YYYY-MM-DD')::TEXT
        -- YYYYMMDD format (8 digits, no separators)
        WHEN CloseDate ~ '^\d{8}$' THEN TO_DATE(CloseDate, 'YYYYMMDD')::TEXT
        -- MM/DD/YYYY format
        WHEN CloseDate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(CloseDate, 'MM/DD/YYYY')::TEXT
        -- DD.MM.YYYY format
        WHEN CloseDate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(CloseDate, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN Amount IS NULL OR TRIM(Amount) = '' OR UPPER(TRIM(Amount)) = 'NONE' THEN NULL
        WHEN TRIM(Amount) = '0' OR TRIM(Amount) = '-0' THEN 0.0
        ELSE CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(Amount, '^\s*[A-Za-z€$£]+\s*', '', ''),
                    '\.', '', -- remove thousand-separator dots (European format)
                    'g'
                )::TEXT,
                ',', '.', -- swap comma to decimal point
                'g'
            )::DOUBLE PRECISION
        END
    END AS "Amount",
    CASE
        WHEN CurrencyIsoCode IS NULL THEN NULL
        WHEN UPPER(TRIM(CurrencyIsoCode)) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(CurrencyIsoCode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(CurrencyIsoCode)) = 'CHF' THEN 'CHF'
        WHEN UPPER(TRIM(CurrencyIsoCode)) = 'GBP' THEN 'GBP'
        ELSE UPPER(TRIM(CurrencyIsoCode))
    END AS "CurrencyIsoCode",
    AccountId,
    Id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}