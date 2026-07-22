-- depends_on: {{ source('fixture_messy_data_v2_src', 'opportunity') }}

{{ config(materialized='table') }}

WITH prepped_opportunity_data AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        TRIM(REGEXP_REPLACE(TRIM(amount), '[^0-9.,-]', '', 'g')) AS pre_cleaned_amount_str
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.stagename) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(o.stagename) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(o.stagename) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(o.stagename) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(o.stagename) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(o.stagename) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(o.stagename) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(o.stagename) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(o.stagename) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(o.stagename) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for NOT NULL enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.closedate, 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(o.closedate, 'MM/DD/YYYY')
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.closedate, 'DD.MM.YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default date if unparseable, as CloseDate is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN o.pre_cleaned_amount_str ~ '^-?\d{1,3}(\.\d{3})*,\d+$' THEN -- European format (e.g., 1.234,56)
            REPLACE(REPLACE(o.pre_cleaned_amount_str, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN o.pre_cleaned_amount_str ~ '^-?\d{1,3}(,\d{3})*\.\d+$' THEN -- US format (e.g., 1,234.56)
            REPLACE(o.pre_cleaned_amount_str, ',', '')::DOUBLE PRECISION
        WHEN o.pre_cleaned_amount_str ~ '^-?\d+,\d+$' THEN -- Simple European without thousand separators (e.g., 1234,56)
            REPLACE(o.pre_cleaned_amount_str, ',', '.')::DOUBLE PRECISION
        WHEN o.pre_cleaned_amount_str ~ '^-?\d+\.?\d*$' THEN -- Simple US format or integer (e.g., 1234.56, 1234)
            o.pre_cleaned_amount_str::DOUBLE PRECISION
        ELSE
            NULL
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    prepped_opportunity_data AS o