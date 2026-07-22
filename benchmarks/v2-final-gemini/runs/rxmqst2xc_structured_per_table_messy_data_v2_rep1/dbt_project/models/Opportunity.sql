-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN src.stagename IS NULL THEN 'Prospecting'
        WHEN TRIM(src.stagename) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(src.stagename) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(src.stagename) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(src.stagename) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(src.stagename) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(src.stagename) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(src.stagename) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(src.stagename) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(src.stagename) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(src.stagename) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unrecognized stage names
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(src.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(src.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(src.closedate), 'DD-MM-YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(src.closedate), 'YYYYMMDD'), 'YYYY-MM-DD'),
        '1900-01-01' -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN TRIM(src.amount) ~ '^\d+(\.\d{3})*,\d+$' THEN -- European format (e.g., 1.234,56)
            REPLACE(REPLACE(TRIM(src.amount), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(src.amount) ~ '^\d+\.?\d*$' THEN -- US format (e.g., 1234.56 or 1234)
            TRIM(src.amount)::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(src.currencyisocode) AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src