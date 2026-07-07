-- depends_on: {{ source('fixture_messy_data_v2_src', 'opportunity') }}
{{ config(materialized='table') }}

WITH source_opportunity AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),

cleaned_opportunity AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        -- Clean and parse Amount
        CASE
            WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN NULL
            ELSE
                -- Remove all characters that are not digits, comma, or period
                CASE
                    WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g') ~ '^\s*$' THEN NULL -- If only garbage remains
                    ELSE
                        -- Check if it's a European format (dot for thousands, comma for decimal)
                        CASE
                            WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g') LIKE '%.%,'
                                 AND STRPOS(REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g'), '.') < STRPOS(REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g'), ',')
                            THEN
                                -- Remove dots (thousand separators), then replace comma with dot (decimal)
                                CAST(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g'), '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
                            ELSE
                                -- Assume standard (comma for thousands or no thousands, dot for decimal, or comma as decimal)
                                -- Remove commas if they are thousand separators or if only comma is decimal, then replace with dot
                                CAST(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
                        END
                END
        END AS parsed_amount,

        -- Parse CloseDate
        COALESCE(
            CASE
                WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate -- YYYY-MM-DD
                WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
                WHEN closedate ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD-MM-YYYY'), 'YYYY-MM-DD')
                ELSE NULL -- Prefer NULL for unparseable dates
            END,
            TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Non-sentinel default for NOT NULL target column
        ) AS parsed_closedate

    FROM source_opportunity
)

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NULL or unmatchable values, target is NOT NULL
    END AS "StageName",
    parsed_closedate AS "CloseDate",
    parsed_amount AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId", -- AccountId maps directly to source account.id
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunity