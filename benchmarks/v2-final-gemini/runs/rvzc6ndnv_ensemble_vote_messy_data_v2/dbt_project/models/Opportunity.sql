{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stagename)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stagename)) = 'lost' THEN 'Closed Lost' -- map 'LOST' to 'Closed Lost'
        WHEN LOWER(TRIM(o.stagename)) = 'prospect' THEN 'Prospecting' -- map 'Prospect' to 'Prospecting'
        ELSE 'Prospecting' -- Default for unmapped values
    END AS "StageName",
    COALESCE(
        -- YYYY-MM-DD
        CASE WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate END,
        -- YYYYMMDD
        CASE WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD') END,
        -- DD.MM.YYYY
        CASE WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') END,
        -- M/D/YYYY or MM/DD/YYYY (assuming US format M/D/YYYY for ambiguity)
        CASE WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') END,
        '1900-01-01' -- Default for NOT NULL dates
    ) AS "CloseDate",
    NULLIF(
        CASE
            WHEN TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.,-]', '', 'g')) ~ '^[+-]?\d{1,3}(\.\d{3})*,\d+$' -- European format (e.g., 1.234.567,89)
            THEN CAST(REPLACE(REPLACE(TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.,-]', '', 'g')), '.', ''), ',', '.') AS DOUBLE PRECISION)
            WHEN TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.,-]', '', 'g')) ~ '^[+-]?\d{1,3}(,\d{3})*\.\d+$' -- US format (e.g., 1,234,567.89)
            THEN CAST(REPLACE(TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.,-]', '', 'g')), ',', '') AS DOUBLE PRECISION)
            WHEN TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.-]', '', 'g')) ~ '^[+-]?\d*\.?\d+$' -- Simple decimal (no thousands separator)
            THEN CAST(TRIM(REGEXP_REPLACE(o.amount, '[^0-9\.-]', '', 'g')) AS DOUBLE PRECISION)
            ELSE NULL
        END,
        ''
    ) AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o
