{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stagename) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(o.stagename) = 'qualification' THEN 'Qualification'
        WHEN LOWER(o.stagename) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(o.stagename) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(o.stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stagename) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(o.stagename) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stagename) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(o.stagename) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(o.stagename) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    CASE
        WHEN TRIM(o.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(o.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(o.closedate) ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(o.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default date for NOT NULL
    END AS "CloseDate",
    CASE
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE
            NULLIF(
                TRIM(
                    CASE
                        -- Case: European decimal (comma last, dot as thousand sep) e.g., '1.234,56'
                        WHEN POSITION(',' IN o.amount) > 0 AND POSITION('.' IN o.amount) > 0
                             AND POSITION(',' IN o.amount) > POSITION('.' IN o.amount) THEN
                            REPLACE(REPLACE(TRIM(REGEXP_REPLACE(o.amount, '[^0-9,.]', '', 'g')), '.', ''), ',', '.')
                        -- Case: American decimal (dot last, comma as thousand sep) e.g., '1,234.56'
                        WHEN POSITION('.' IN o.amount) > 0 AND POSITION(',' IN o.amount) > 0
                             AND POSITION('.' IN o.amount) > POSITION(',' IN o.amount) THEN
                            REPLACE(TRIM(REGEXP_REPLACE(o.amount, '[^0-9,.]', '', 'g')), ',', '')
                        -- Case: Only comma, assume European decimal e.g., '1234,56'
                        WHEN POSITION(',' IN o.amount) > 0 AND POSITION('.' IN o.amount) = 0 THEN
                            REPLACE(TRIM(REGEXP_REPLACE(o.amount, '[^0-9,]', '', 'g')), ',', '.')
                        -- Case: Only dot, assume American decimal e.g., '1234.56'
                        WHEN POSITION('.' IN o.amount) > 0 AND POSITION(',' IN o.amount) = 0 THEN
                            TRIM(REGEXP_REPLACE(o.amount, '[^0-9.]', '', 'g'))
                        -- Case: Only digits, no separators
                        WHEN TRIM(o.amount) ~ '^[\d]+$' THEN
                            TRIM(o.amount)
                        ELSE NULL
                    END
                ), ''
            )::DOUBLE PRECISION
    END AS "Amount",
    TRIM(UPPER(o.currencyisocode)) AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o
