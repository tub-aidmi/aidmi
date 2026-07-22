{{ config(materialized='table') }}

SELECT
    TRIM(source.id) AS "Id",
    COALESCE(NULLIF(TRIM(source.name), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN INITCAP(TRIM(source.stagename)) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN INITCAP(TRIM(source.stagename))
        ELSE 'Prospecting' -- Default value for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN TRIM(source.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(source.closedate), 'YYYY-MM-DD')
                WHEN TRIM(source.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(source.closedate), 'DD.MM.YYYY')
                WHEN TRIM(source.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(source.closedate), 'MM/DD/YYYY')
                WHEN TRIM(source.closedate) ~ '^\d{8}$' THEN TO_DATE(TRIM(source.closedate), 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN TRIM(source.amount) IS NULL OR TRIM(source.amount) = '' THEN NULL
        ELSE
            -- Attempt to clean for European format first
            CASE
                WHEN REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.,]+', '', 'g') ~ '^\d{1,3}(\.\d{3})*,\d+$' THEN
                    CAST(REPLACE(REPLACE(REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.,]+', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
                ELSE
                    -- If not European format, try cleaning for US format.
                    -- Get the string cleaned for US format (remove all non-digits, except dot)
                    CASE
                        WHEN REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.]+', '', 'g') = '' THEN NULL -- If it becomes empty, it's not a valid number
                        ELSE CAST(REPLACE(REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.]+', '', 'g'), ',', '') AS DOUBLE PRECISION)
                    END
            END
    END AS "Amount",
    TRIM(source.currencyisocode) AS "CurrencyIsoCode",
    TRIM(source.accountid) AS "AccountId",
    TRIM(source.id) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS source
