{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
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
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL target
    END AS "CloseDate",
    CASE
        WHEN TRIM(amount) ~ '^\s*[$€]?\s*\d{1,3}([.,]\d{3})*([.,]\d{1,2})?\s*$' THEN
            CASE
                WHEN POSITION(',' IN TRIM(amount)) > POSITION('.' IN TRIM(amount)) THEN
                    -- European format: 1.234,56 (dot is thousands, comma is decimal)
                    REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9,.]', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                ELSE
                    -- American format: 1,234.56 (comma is thousands, dot is decimal) or no thousands separator
                    REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.]', '', 'g'), ',', '')::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    TRIM(currencyisocode) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using source id as legacy ID
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}