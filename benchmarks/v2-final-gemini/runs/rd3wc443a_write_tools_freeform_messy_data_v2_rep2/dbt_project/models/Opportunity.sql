{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name", -- Name is NOT NULL, fallback to id
    COALESCE(
        CASE LOWER(TRIM(stagename))
            WHEN 'closed won' THEN 'Closed Won'
            WHEN 'won' THEN 'Closed Won'
            WHEN 'gewonnen' THEN 'Closed Won'
            WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
            WHEN 'closed lost' THEN 'Closed Lost'
            WHEN 'lost' THEN 'Closed Lost'
            WHEN 'verloren' THEN 'Closed Lost'
            WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
            WHEN 'prospecting' THEN 'Prospecting'
            WHEN 'prospect' THEN 'Prospecting'
            WHEN 'in kontakt' THEN 'Prospecting'
            WHEN 'qualification' THEN 'Qualification'
            WHEN 'qualifikation' THEN 'Qualification'
            WHEN 'quali' THEN 'Qualification'
            WHEN 'in prüfung' THEN 'Qualification'
            WHEN 'needs analysis' THEN 'Needs Analysis'
            WHEN 'value proposition' THEN 'Value Proposition'
            WHEN 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN 'perception analysis' THEN 'Perception Analysis'
            WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN 'negotiation/review' THEN 'Negotiation/Review'
            ELSE 'Prospecting' -- Default for NOT NULL column if no match
        END,
        'Prospecting' -- Final fallback if CASE returns NULL (e.g. stagename was NULL or empty string)
    ) AS "StageName",
    COALESCE(
        (CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
            WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')
            ELSE NULL
        END)::TEXT,
        CURRENT_DATE::TEXT -- Fallback for NOT NULL column
    ) AS "CloseDate",
    CASE
        WHEN TRIM(amount) = '' THEN NULL
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9\.,-]', '', 'g') ~ '^[0-9\.-]+,[0-9]+$' THEN
            CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9\.,-]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9\.-]', '', 'g') ~ '^[0-9\.-]+$' THEN
            CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9\.-]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE LOWER(TRIM(currencyisocode))
        WHEN 'usd' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'dollar' THEN 'USD'
        WHEN 'gbp' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        WHEN 'eur' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'euro' THEN 'EUR'
        WHEN 'chf' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using id as the natural key
    NULL::TEXT AS "CreatedDate", -- Placeholder
    NULL::TEXT AS "LastModifiedDate", -- Placeholder
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
