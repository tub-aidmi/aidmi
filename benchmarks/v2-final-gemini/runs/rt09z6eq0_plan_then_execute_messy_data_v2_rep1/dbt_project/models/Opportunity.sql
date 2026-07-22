{{ config(materialized='table') }}

WITH opportunity_initial AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        -- Initial cleaning for amount: remove common currency symbols and spaces, keep digits, dot, comma, minus
        TRIM(
            REGEXP_REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    UPPER(amount),
                                    'EUR ', ''
                                ),
                                '$', ''
                            ),
                            '£', ''
                        ),
                        '¥', ''
                    ),
                    'CHF ', ''
                ),
                '[^0-9.,-]', '', 'g' -- Remove anything that's not a digit, dot, comma, or minus
            )
        ) AS cleaned_amount_step1
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(o.stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped/NULL values to satisfy NOT NULL constraint
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default for NULL or unparseable dates to satisfy NOT NULL constraint
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN o.cleaned_amount_step1 IS NULL OR o.cleaned_amount_step1 = '' THEN NULL
            WHEN o.cleaned_amount_step1 ~ '^[+-]?\d+\.\d{3},\d+$' THEN -- European with thousand separator (e.g., 1.234,56)
                REPLACE(REPLACE(o.cleaned_amount_step1, '.', ''), ',', '.')
            WHEN o.cleaned_amount_step1 ~ '^[+-]?\d+,\d+$' THEN -- European/Indian decimal (e.g., 123,45)
                REPLACE(o.cleaned_amount_step1, ',', '.')
            WHEN o.cleaned_amount_step1 ~ '^[+-]?\d{1,3}(,\d{3})*\.\d+$' THEN -- US with thousand separator (e.g., 1,234.56)
                REPLACE(o.cleaned_amount_step1, ',', '')
            ELSE o.cleaned_amount_step1 -- Standard decimal or integer (e.g., 123.45, 123)
        END AS TEXT
    ) AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_initial AS o