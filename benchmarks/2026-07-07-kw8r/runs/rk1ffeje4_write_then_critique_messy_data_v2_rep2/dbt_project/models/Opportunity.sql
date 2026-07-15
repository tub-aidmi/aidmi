{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
stripped AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        -- Strip currency text/prefixes from amount for cleaner parsing
        REGEXP_REPLACE(TRIM(amount), '[A-Za-z\s€£$,]+', '', 'g') AS clean_amount,
        -- Strip currency prefix/suffix from currency code field
        TRIM(currencyisocode) AS clean_currency
    FROM source
),
cleaned AS (
    SELECT
        -- Id: keep original format (OPP-XXXXX)
        TRIM(id) AS "Id",

        -- Name: trim whitespace
        TRIM(name) AS "Name",

        -- StageName: normalize all variations to enum values; NULL for unmapped
        CASE
            WHEN LOWER(TRIM(stagename)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
            WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
            WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
            WHEN LOWER(TRIM(stagename)) = 'in prüfung' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'wertversprechen') THEN 'Value Proposition'
            WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identifizierung entscheidungsträger') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
                -- German wrapped formats (e.g. "Abgeschlossen (Gewonnen)") — after exact matches
            WHEN stagename ILIKE '%gewonnen%' THEN 'Closed Won'
            WHEN stagename ILIKE '%verloren%' THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",

        -- CloseDate: parse multiple formats into YYYY-MM-DD; NULL for unparseable (no sentinel)
        CASE
            WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
            WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
            WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN TRIM(closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "CloseDate",

        -- Amount: handle European (dot-thousand-sep + comma-decimal) and US decimal formats; NULL for unparseable or "None"
        CASE
            WHEN clean_amount IS NULL OR TRIM(clean_amount) = '' THEN NULL
            WHEN LOWER(TRIM(amount)) = 'none' THEN NULL
                -- European format with thousand-separator dots and decimal comma: 60.702,05 → 60702.05
                -- Order is critical: remove dots first (thousand sep), then swap comma→dot (decimal)
            WHEN clean_amount ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
                REPLACE(REPLACE(clean_amount, '.', ''), ',', '.')::DOUBLE PRECISION
                -- Decimal comma without thousand separators: 1234,56 → 1234.56
            WHEN clean_amount ~ '^\-?\d+,\d+$' THEN
                REPLACE(clean_amount, ',', '.')::DOUBLE PRECISION
                -- Standard US decimal with dot: 476276.13, -120228.71
            WHEN clean_amount ~ '^\-?\d+\.\d+$' THEN
                clean_amount::DOUBLE PRECISION
                -- Plain integer: 0, etc.
            WHEN clean_amount ~ '^\-?\d+$' THEN
                clean_amount::DOUBLE PRECISION
            ELSE NULL
        END AS "Amount",

        -- CurrencyIsoCode: map various formats to ISO 4217 codes; NULL for unmapped
        CASE
            WHEN LOWER(TRIM(clean_currency)) IN ('eur', 'euro', '€') THEN 'EUR'
            WHEN LOWER(TRIM(clean_currency)) IN ('usd', 'dollar', '$') THEN 'USD'
            WHEN UPPER(TRIM(clean_currency)) IN ('CHF') THEN 'CHF'
            WHEN LOWER(TRIM(clean_currency)) IN ('gbp', '£') THEN 'GBP'
            ELSE NULL
        END AS "CurrencyIsoCode",

        -- AccountId: CUST-XXXX format matches source; reference to transformed Account.Id
        TRIM(accountid) AS "AccountId",

        -- Legacy_Opportunity_ID__c: populate from source natural key
        TRIM(id) AS "Legacy_Opportunity_ID__c"

    FROM stripped
)

SELECT
    "Id",
    "Name",
    "StageName",
    "CloseDate",
    "Amount",
    "CurrencyIsoCode",
    "AccountId",
    "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM cleaned;