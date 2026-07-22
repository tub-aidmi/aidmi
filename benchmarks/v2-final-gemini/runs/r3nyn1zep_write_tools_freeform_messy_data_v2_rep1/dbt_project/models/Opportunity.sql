{{ config(materialized='table') }}

WITH cleaned_opportunity AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        -- Clean the amount string first
        CASE
            WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN NULL
            ELSE
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                TRIM(amount),
                                '€', ''
                            ),
                            '$', ''
                        ),
                        '£', ''
                    ),
                    ' ', '' -- Remove spaces
                )
        END AS cleaned_amount_str
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN LOWER(stagename) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(stagename) = 'qualification' THEN 'Qualification'
        WHEN LOWER(stagename) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(stagename) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(stagename) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(stagename) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(stagename) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(stagename) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(stagename) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1970-01-01' -- Default to satisfy NOT NULL constraint if unparseable
    ) AS "CloseDate",
    CASE
        WHEN cleaned_amount_str IS NULL THEN NULL
        -- European format (e.g., 1.234,56 or 123,45)
        WHEN cleaned_amount_str ~ '^\d*\.?\d*,\d{1,2}$' THEN
            CAST(REPLACE(REPLACE(cleaned_amount_str, '.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        -- American format (e.g., 1,234.56 or 123.45)
        WHEN cleaned_amount_str ~ '^\d*,?\d*\.\d{1,2}$' THEN
            CAST(REPLACE(cleaned_amount_str, ',', '', 'g') AS DOUBLE PRECISION)
        -- Pure integer or American without decimal (e.g., 1234 or 1,234)
        WHEN cleaned_amount_str ~ '^\d+(,\d{3})*$' THEN
            CAST(REPLACE(cleaned_amount_str, ',', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunity
