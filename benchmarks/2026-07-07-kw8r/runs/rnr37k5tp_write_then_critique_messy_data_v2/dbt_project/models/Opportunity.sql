{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('in prüfung', 'in pruefung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'decision makers', 'decision maker') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- Parse CloseDate to ISO YYYY-MM-DD from multiple source formats; prefer NULL over sentinel dates
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{8}$' THEN
            SUBSTRING(closedate, 1, 4) || '-' ||
            SUBSTRING(closedate, 5, 2) || '-' ||
            SUBSTRING(closedate, 7, 2)
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            SUBSTRING(closedate FROM 7 FOR 4) || '-' ||
            SUBSTRING(closedate FROM 4 FOR 2) || '-' ||
            SUBSTRING(closedate FROM 1 FOR 2)
        WHEN closedate ~ '^\d+/\d+/\d{4}$' THEN
            SPLIT_PART(closedate, '/', 3) || '-' ||
            LPAD(SPLIT_PART(closedate, '/', 1), 2, '0') || '-' ||
            LPAD(SPLIT_PART(closedate, '/', 2), 2, '0')
        ELSE NULL
    END AS "CloseDate",

    -- Parse Amount: handle European format (dot-comma), currency-prefixed values; strip trailing dot for whole numbers
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' OR LOWER(TRIM(amount)) = 'none' THEN NULL::DOUBLE PRECISION
        ELSE
            CAST(
                CASE
                    -- Strip leading currency text/symbols (case-insensitive) then detect format
                    WHEN REGEXP_REPLACE(
                        TRIM(amount),
                        '^\s*(EUR\s*|USD\s*|CHF\s*|GBP\s*|EURO\s*|DOLLAR\s*|€\s*|\$\s*|£\s*)',
                        '',
                        'g'
                    ) ~ '[0-9]+\.[0-9]{3},[0-9]' THEN
                        REGEXP_REPLACE(
                            REPLACE(
                                REGEXP_REPLACE(
                                    TRIM(amount),
                                    '^\s*(EUR\s*|USD\s*|CHF\s*|GBP\s*|EURO\s*|DOLLAR\s*|€\s*|\$\s*|£\s*)',
                                    '',
                                    'g'
                                ),
                                '\.', ''
                            ),
                            ',', '.'
                        )
                    -- US format or already-cleaned: strip any remaining commas (thousand-seps)
                    ELSE REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(amount),
                            '^\s*(EUR\s*|USD\s*|CHF\s*|GBP\s*|EURO\s*|DOLLAR\s*|€\s*|\$\s*|£\s*)',
                            '',
                            'g'
                        ),
                        ',', ''
                    )
                END
            AS DOUBLE PRECISION)
    END AS "Amount",

    -- Normalize currency ISO codes
    CASE
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",

    -- AccountId: consistent TRIM without LOWER to match Account.Id format exactly
    TRIM(accountid) AS "AccountId",

    -- Legacy natural key for row-level verification
    id AS "Legacy_Opportunity_ID__c",

    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}