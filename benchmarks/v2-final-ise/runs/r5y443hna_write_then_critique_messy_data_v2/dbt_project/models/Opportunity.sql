{{ config(materialized='table') }}

SELECT
    -- Id: apply Salesforce-style '006' prefix for cross-table alignment with Opportunity references
    '006' || REGEXP_REPLACE(TRIM(id), '[^0-9]', '', 'g') AS "Id",

    -- Name: normalize and ensure NOT NULL with a fallback
    COALESCE(TRIM(INITCAP(name)), 'Unnamed Opportunity') AS "Name",

    -- StageName: map source stagename to target enum values; fallback 'Prospecting' (valid enum member) for unmatched rows
    CASE
        WHEN LOWER(TRIM(stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: parse multiple date formats to YYYY-MM-DD; fallback NULL for missing or unparseable dates
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN CAST(TRIM(closedate) AS DATE)::TEXT
        WHEN TRIM(closedate) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
        WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: strip currency symbols, remove thousand-separator dots, swap decimal comma to dot, then cast to DOUBLE PRECISION
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        WHEN amount ~ '^[\€$£]?\s*\d{1,3}(\.\d{3})+\,\d+$' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[\€$£]', '', 'g'), '\.', '', 'g'),
                      ',', '.', 'g'
                  ) AS DOUBLE PRECISION
              )
        WHEN amount ~ '^[\€$£]?\s*\d{1,3}(,\d{3})+\.\d+$' THEN
            CAST(
                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[\€$£]', '', 'g'), ',', '', 'g') AS DOUBLE PRECISION
              )
        WHEN amount ~ '^[\€$£]?\s*\d+\.\d*$' THEN
            CAST(REGEXP_REPLACE(TRIM(amount), '[\€$£]', '', 'g') AS DOUBLE PRECISION)
        WHEN amount ~ '^[\€$£]?\s*\d+$' THEN
            CAST(REGEXP_REPLACE(TRIM(amount), '[\€$£]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: normalize currency codes to uppercase 3-letter ISO format
    CASE
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', 'us dollar', 'us dollars') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', 'euros') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', 'british pound', 'sterling') THEN 'GBP'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf', 'swiss franc', 'swiss francs') THEN 'CHF'
        WHEN LOWER(TRIM(currencyisocode)) IN ('cad', 'canadian dollar', 'canadian dollars') THEN 'CAD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('jpy', 'yen') THEN 'JPY'
        WHEN UPPER(TRIM(currencyisocode)) ~ '^[A-Z]{3}$' THEN UPPER(TRIM(currencyisocode))
        ELSE NULL
    END AS "CurrencyIsoCode",

    -- AccountId: apply Salesforce-style key transform to match Account.Id for cross-table alignment
    '001' || REGEXP_REPLACE(TRIM(accountid), '[^0-9]', '', 'g') AS "AccountId",

    -- Legacy_Opportunity_ID__c: preserve original source id for audit trail
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",

    -- Dates not available in source data — default to NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: no soft-delete concept in source, default to 0
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
WHERE id IS NOT NULL