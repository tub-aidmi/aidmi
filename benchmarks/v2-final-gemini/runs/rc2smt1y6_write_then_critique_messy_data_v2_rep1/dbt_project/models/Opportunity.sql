-- depends_on: {{ source('fixture_messy_data_v2_src', 'opportunity') }}
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.stagename) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
        THEN TRIM(o.stagename)
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(o.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.closedate, 'DD-MM-YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE
            -- Attempt to clean European format (dot as thousand, comma as decimal)
            -- Then clean US format (comma as thousand, dot as decimal)
            COALESCE(
                CAST(REPLACE(REPLACE(o.amount, '.', ''), ',', '.') AS DOUBLE PRECISION),
                CAST(REPLACE(o.amount, ',', '') AS DOUBLE PRECISION)
            )
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o