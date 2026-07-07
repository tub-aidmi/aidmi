{{ config(materialized='table') }}

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
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate -- YYYY-MM-DD
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            ELSE NULL
        END,
        '1900-01-01' -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g') ~ '^[0-9]+(\.[0-9]{3})*,[0-9]{2}$' THEN -- European format (e.g., 1.234.567,89)
            CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g') ~ '^[0-9]+(,[0-9]{3})*\.[0-9]{2}$' THEN -- US format (e.g., 1,234,567.89)
            CAST(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g'), ',', '') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g') ~ '^[0-9]+,[0-9]+$' THEN -- Comma as decimal, no thousands
            CAST(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.]', '', 'g') ~ '^[0-9]+\.[0-9]+$' THEN -- Dot as decimal, no thousands
            CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9]', '', 'g') ~ '^[0-9]+$' THEN -- Only digits
            CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using source ID as legacy ID
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}