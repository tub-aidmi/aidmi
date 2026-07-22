{{ config(materialized='table') }}

WITH cleaned_amount AS (
    SELECT
        o."id",
        o."name",
        o."stagename",
        o."closedate",
        o."amount",
        o."currencyisocode",
        o."accountid",

        /* StageName: normalize to target enum */
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

        /* CloseDate: parse multiple formats to ISO YYYY-MM-DD */
        CASE
            WHEN TRIM(o."closedate") IS NULL OR TRIM(o."closedate") = '' THEN NULL
            WHEN TRIM(o."closedate") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(o."closedate")
            WHEN TRIM(o."closedate") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o."closedate"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(o."closedate") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o."closedate"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(o."closedate") ~ '^\d{8}$' THEN CONCAT(SUBSTRING(TRIM(o."closedate"), 1, 4), '-', SUBSTRING(TRIM(o."closedate"), 5, 2), '-', SUBSTRING(TRIM(o."closedate"), 7, 2))
            ELSE NULL
        END AS "CloseDate",

        /* Amount: robust handling of US/European formats, strip currency symbols */
        CASE
            WHEN TRIM(o."amount") IS NULL OR TRIM(o."amount") = '' THEN NULL
            ELSE
                CASE
                    -- European format: 1.234,56 (dots=thousands, comma=decimal)
                    WHEN REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
                        CAST(
                            REPLACE(
                                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g'), '\.', ''),
                                ',', '.'
                            ) AS DOUBLE PRECISION
                        )
                    -- US format: 1,234.56 (commas=thousands, dot=decimal)
                    WHEN REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') ~ '^\-?\d{1,3}(,\d{3})+\.\d+$' THEN
                        CAST(
                            REPLACE(REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g'), ',', '') AS DOUBLE PRECISION
                        )
                    -- Simple decimal with dot: 1234.56
                    WHEN REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') ~ '^\-?\d+\.\d+$' THEN
                        CAST(REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') AS DOUBLE PRECISION)
                    -- European decimal with comma: 1234,56
                    WHEN REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') ~ '^\-?\d+,\d+$' THEN
                        CAST(REPLACE(REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g'), ',', '.') AS DOUBLE PRECISION)
                    -- Plain integer: 1234
                    WHEN REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') ~ '^\-?\d+$' THEN
                        CAST(REGEXP_REPLACE(TRIM(o."amount"), '^\s*[€$£]?', '', 'g') AS DOUBLE PRECISION)
                    ELSE NULL
                END
        END AS "Amount",

        UPPER(TRIM(COALESCE(o."currencyisocode", ''))) AS "CurrencyIsoCode"

    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
),

account_mapping AS (
    SELECT
        a."id" AS account_id_raw,
        a."id" AS sf_account_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }} a
)

SELECT
    CAST(ca."id" AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(ca."name"), ''), 'Unnamed Opportunity') AS "Name",
    ca."StageName",
    COALESCE(ca."CloseDate", '') AS "CloseDate",
    ca."Amount",
    ca."CurrencyIsoCode",

    /* Join to account to resolve Salesforce Account Id */
    am.sf_account_id AS "AccountId",

    CAST(ca."id" AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM cleaned_amount ca
LEFT JOIN account_mapping am
    ON TRIM(am.account_id_raw) = TRIM(ca."accountid")