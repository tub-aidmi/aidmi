{{ config(materialized='table') }}

WITH opp_prep AS (
    SELECT
        op.*,
        CASE
            WHEN TRIM(LOWER(op.amount)) IN ('', 'null', 'none', 'n/a') OR op.amount IS NULL THEN ''
            ELSE REGEXP_REPLACE(TRIM(op.amount), '[^\d.,]', '', 'g')
        END AS amount_cleaned
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} op
)

SELECT
    CAST(op.id AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(op.name), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(op.stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(op.stagename)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(op.stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(op.stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(op.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(op.stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(op.stagename)) IN ('proposal/price quote', 'proposal / price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(op.stagename)) IN ('negotiation/review', 'negotiation / review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(op.stagename)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(op.stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN op.closedate IS NULL OR TRIM(op.closedate) = '' THEN NULL
        WHEN op.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(op.closedate), 'DD.MM.YYYY')::TEXT
        WHEN op.closedate ~ '^\d{8}$' THEN SUBSTR(TRIM(op.closedate), 1, 4) || '-' || SUBSTR(TRIM(op.closedate), 5, 2) || '-' || SUBSTR(TRIM(op.closedate), 7, 2)
        WHEN op.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(op.closedate), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN op.amount_cleaned = '' OR op.amount_cleaned IS NULL THEN NULL
        ELSE CAST(
            CASE
                WHEN op.amount_cleaned ~ '\.' AND op.amount_cleaned ~ ',' THEN
                    REPLACE(REPLACE(op.amount_cleaned, '.', ''), ',', '.')
                WHEN op.amount_cleaned ~ ',' AND NOT op.amount_cleaned ~ '\.' THEN
                    REPLACE(op.amount_cleaned, ',', '.')
                ELSE
                    op.amount_cleaned
            END
        AS DOUBLE PRECISION)
    END AS "Amount",
    NULLIF(TRIM(UPPER(op.currencyisocode)), '') AS "CurrencyIsoCode",
    a.id AS "AccountId",
    CAST(op.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM opp_prep op
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON TRIM(LOWER(op.accountid)) = TRIM(LOWER(a.id))