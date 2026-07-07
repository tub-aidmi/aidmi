-- noinspection SqlNoDataSourceInspection
-- noinspection SqlDialectInspection
{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
account_source AS (
    SELECT
        id,
        legacy_customer_id__c
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)
SELECT
    os.id AS "Id",
    COALESCE(os.name, os.id) AS "Name",
    CASE
        WHEN UPPER(TRIM(os.stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(os.stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(os.stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(os.stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(os.stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(os.stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(os.stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(os.stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(os.stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(os.stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    COALESCE(
        CASE
            WHEN os.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN os.closedate -- YYYY-MM-DD
            WHEN os.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(os.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN os.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(os.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN os.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(os.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL target column
    ) AS "CloseDate",
    CASE
        WHEN os.amount IS NULL OR TRIM(os.amount) = '' THEN NULL
        ELSE
            CAST(
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM(os.amount), '[^0-9,\.]+', '', 'g'),
                        '.',
                        ''
                    ),
                    ',',
                    '.'
                ) AS DOUBLE PRECISION
            )
    END AS "Amount",
    os.currencyisocode AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    os.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_source os
LEFT JOIN account_source acc ON os.accountid = acc.legacy_customer_id__c;