{{ config(materialized='table') }}

SELECT
    t.id AS "Id",
    COALESCE(TRIM(t.name), 'N/A') AS "Name",
    CASE
        WHEN UPPER(TRIM(t.stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(t.stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(t.stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(t.stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(t.stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(t.stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(t.stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(t.stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(t.stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(t.stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            CASE WHEN TRIM(t.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(t.closedate), 'DD.MM.YYYY') END,
            CASE WHEN TRIM(t.closedate) ~ '^\d{8}$' THEN TO_DATE(TRIM(t.closedate), 'YYYYMMDD') END,
            CASE WHEN TRIM(t.closedate) ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_DATE(TRIM(t.closedate), 'MM/DD/YYYY') END,
            '1900-01-01'::DATE -- Default for NOT NULL target column if all parsing fails
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    CASE
        WHEN t.amount IS NULL OR TRIM(t.amount) = '' THEN NULL
        ELSE
            CASE
                -- Scenario 1: European format with dot as thousand and comma as decimal (e.g., '1.234,56')
                WHEN REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g') ~ '^(-?\d+)(\.\d{3})*,\d+$' THEN
                    REPLACE(REPLACE(REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                -- Scenario 2: European format with only comma as decimal (e.g., '123,45')
                WHEN REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g') ~ '^(-?\d+),\d+$' THEN
                    REPLACE(REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g'), ',', '.')::DOUBLE PRECISION
                -- Scenario 3: US format with comma as thousand and dot as decimal (e.g., '1,234.56')
                WHEN REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g') ~ '^(-?\d{1,3}(,\d{3})*\.\d+)$' THEN
                    REPLACE(REGEXP_REPLACE(TRIM(t.amount), '[^0-9.,-]+', '', 'g'), ',', '')::DOUBLE PRECISION
                -- Scenario 4: Standard format with dot as decimal or integer (e.g., '1234.56' or '1234')
                WHEN REGEXP_REPLACE(TRIM(t.amount), '[^0-9.-]+', '', 'g') ~ '^(-?\d+(\.\d+)?)$' THEN
                    REGEXP_REPLACE(TRIM(t.amount), '[^0-9.-]+', '', 'g')::DOUBLE PRECISION
                ELSE NULL
            END
    END AS "Amount",
    UPPER(TRIM(t.currencyisocode)) AS "CurrencyIsoCode",
    t.accountid AS "AccountId",
    t.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS t
