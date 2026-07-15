{{ config(materialized='table') }}

SELECT
    CAST(o."id" AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(o."name"), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o."stagename")) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o."stagename")) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o."stagename")) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o."stagename")) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o."stagename")) LIKE '%decision maker%' OR LOWER(TRIM(o."stagename")) = 'identify decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o."stagename")) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o."stagename")) LIKE '%proposal%' OR LOWER(TRIM(o."stagename")) LIKE '%price quote%' OR LOWER(TRIM(o."stagename")) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o."stagename")) LIKE '%negotiat%' OR LOWER(TRIM(o."stagename")) LIKE '%review%' OR LOWER(TRIM(o."stagename")) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o."stagename")) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o."stagename")) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    COALESCE(
        -- Try ISO 8601: YYYY-MM-DD
        CASE WHEN o."closedate" ~ '^\d{4}-\d{2}-\d{2}$'
            THEN CAST(o."closedate" AS DATE)::TEXT
            ELSE NULL END,
        -- Try DD.MM.YYYY (European)
        CASE WHEN o."closedate" ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(o."closedate", 'DD.MM.YYYY')::TEXT
            ELSE NULL END,
        -- Try MM/DD/YYYY (US)
        CASE WHEN o."closedate" ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(o."closedate", 'MM/DD/YYYY')::TEXT
            ELSE NULL END,
        -- Try YYYYMMDD
        CASE WHEN o."closedate" ~ '^\d{8}$'
            THEN CONCAT(
                SUBSTRING(o."closedate", 1, 4), '-',
                SUBSTRING(o."closedate", 5, 2), '-',
                SUBSTRING(o."closedate", 7, 2)
            )
            ELSE NULL END,
        ''
    ) AS "CloseDate",
    CASE
        WHEN o."amount" IS NULL OR TRIM(o."amount") = '' THEN NULL
        -- European format: dots as thousand separator, comma as decimal (e.g., '1.234,56')
        WHEN o."amount" ~ '^\s*\D?\-?\d{1,3}(\.\d{3})+,\d+\s*$' THEN
            REGEXP_REPLACE(REGEXP_REPLACE(o."amount", '^\s*\D?', '', 'g'), ',([^,]*)$', '.', 'g')::DOUBLE PRECISION
        -- US format: commas as thousand separator (e.g., '$1,234.56' or '1,234.56')
        WHEN o."amount" ~ '^\s*\D?\-?\d{1,3}(,\d{3})+\.\d+\s*$' THEN
            REGEXP_REPLACE(REGEXP_REPLACE(o."amount", '^\s*\D?', '', 'g'), ',', '', 'g')::DOUBLE PRECISION
        -- Plain number or with single decimal (e.g., '1234.56', '$1234', '1234,56')
        WHEN o."amount" ~ '^\s*\D?\-?\d+,\d+\s*$' THEN
            REGEXP_REPLACE(REGEXP_REPLACE(o."amount", '^\s*\D?', '', 'g'), ',', '.', 'g')::DOUBLE PRECISION
        -- Plain number possibly with currency prefix
        WHEN o."amount" ~ '^\s*\D?\-?\d+(\.\d+)?\s*$' THEN
            REGEXP_REPLACE(o."amount", '^\s*\D?', '', 'g')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(COALESCE(o."currencyisocode", ''))) AS "CurrencyIsoCode",
    COALESCE(NULLIF(TRIM(o."accountid"), ''), NULL) AS "AccountId",
    CAST(o."id" AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o;