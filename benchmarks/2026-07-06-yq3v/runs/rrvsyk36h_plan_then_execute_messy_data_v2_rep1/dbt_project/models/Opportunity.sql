{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN INITCAP(TRIM(stagename)) = 'Prospecting' THEN 'Prospecting'
        WHEN INITCAP(TRIM(stagename)) = 'Qualification' THEN 'Qualification'
        WHEN INITCAP(TRIM(stagename)) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN INITCAP(TRIM(stagename)) = 'Value Proposition' THEN 'Value Proposition'
        WHEN INITCAP(TRIM(stagename)) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN INITCAP(TRIM(stagename)) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN INITCAP(TRIM(stagename)) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN INITCAP(TRIM(stagename)) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN INITCAP(TRIM(stagename)) = 'Closed Won' THEN 'Closed Won'
        WHEN INITCAP(TRIM(stagename)) = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(closedate::DATE, 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(amount, '[^0-9.,]+', '', 'g'),
                    E'(\\d)\\.(\\d{3})', E'\\1\\2', 'g' -- Remove thousand separators for European format
                ),
                ',+', '.', 'g' -- Replace comma with dot for decimal
            ) ~ '^[0-9]+(\\.[0-9]+)?$'
        THEN
            CAST(REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(amount, '[^0-9.,]+', '', 'g'),
                    E'(\\d)\\.(\\d{3})', E'\\1\\2', 'g'
                ),
                ',+', '.', 'g'
            ) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data
