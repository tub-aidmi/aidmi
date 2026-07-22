{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), TRIM(id)) AS "Name",
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
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate) -- YYYY-MM-DD
        WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    END AS "CloseDate",
    CAST(
        NULLIF( -- Added NULLIF to handle cases where the internal logic results in an empty string after cleaning
            CASE
                WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN NULL
                ELSE
                    -- Step 1: Remove currency symbols and non-numeric characters except '.' and ','
                    CASE
                        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]+', '', 'g') ~ ',([0-9]{1,2})$' THEN -- Heuristic: if last segment is comma followed by 1 or 2 digits, it's likely European decimal
                            REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]+', '', 'g'), '.', ''), ',', '.')
                        ELSE -- Assume US format or integer: remove commas, keep dots
                            REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.]+', '', 'g'), ',', '') -- Remove commas, other non-numeric chars (if any remaining besides dot)
                    END
            END,
            '' -- If the above logic produces an empty string, convert it to NULL before casting
        )
    AS DOUBLE PRECISION) AS "Amount",
    TRIM(currencyisocode) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
