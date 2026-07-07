{{
    config(materialized='table')
}}

WITH prepped_opportunity AS (
    SELECT
        opportunity.id,
        opportunity.name,
        opportunity.stagename,
        opportunity.closedate,
        opportunity.amount,
        opportunity.currencyisocode,
        opportunity.accountid,
        -- Pre-clean the amount string to simplify parsing logic later
        TRIM(REGEXP_REPLACE(TRIM(opportunity.amount), '[^0-9.,]+', '', 'g')) AS cleaned_amount_str
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
)
SELECT
    TRIM(id) AS "Id",
    TRIM(COALESCE(name, 'Unknown Opportunity')) AS "Name",
    CASE
        WHEN TRIM(stagename) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(stagename) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(stagename) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(stagename) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(stagename) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(stagename) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(stagename) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(stagename) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(stagename) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(stagename) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Fallback for NULL or unmapped values, satisfying NOT NULL
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default value for NOT NULL constraint if all parsing fails
    END AS "CloseDate",
    CAST(
        CASE
            WHEN cleaned_amount_str IS NULL OR cleaned_amount_str = '' THEN NULL
            -- Case 1: Both dot and comma present (e.g., 1.234,56 or 1,234.56)
            WHEN STRPOS(cleaned_amount_str, '.') > 0 AND STRPOS(cleaned_amount_str, ',') > 0 THEN
                CASE
                    -- European format: dot is thousands separator, comma is decimal separator (e.g., 1.234,56)
                    WHEN STRPOS(cleaned_amount_str, '.') < STRPOS(cleaned_amount_str, ',') THEN
                        REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.')
                    -- US format: comma is thousands separator, dot is decimal separator (e.g., 1,234.56)
                    ELSE
                        REPLACE(cleaned_amount_str, ',', '')
                END
            -- Case 2: Only comma present (assume European decimal, e.g., 1234,56)
            WHEN STRPOS(cleaned_amount_str, ',') > 0 THEN
                REPLACE(cleaned_amount_str, ',', '.')
            -- Case 3: Only dot present or no separator (e.g., 1234.56, 12345)
            ELSE
                cleaned_amount_str
        END
    AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM prepped_opportunity