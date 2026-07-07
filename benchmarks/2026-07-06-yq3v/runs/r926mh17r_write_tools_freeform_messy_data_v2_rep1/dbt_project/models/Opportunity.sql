{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')
            WHEN closedate ~ '^\d{2}.\d{2}.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default date if unparseable, since target is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CAST(
                CASE
                    -- Define the cleaned string value once to avoid repetition inside inner CASE conditions
                    WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')) = '' THEN NULL
                    WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')) = '-' THEN NULL
                    ELSE
                        CASE
                            -- Now, apply separator logic using the same cleaned string (repeated for clarity within current constraints)
                            WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')) ~ '^[+-]?\d{1,3}(?:\.\d{3})*,\d+$' THEN -- European: d.ddd,dd
                                REPLACE(REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')), '.', ''), ',', '.')
                            WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')) ~ '^[+-]?\d+,\d+$' THEN -- European: d,dd
                                REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]+', '', 'g')), ',', '.')
                            ELSE -- Standard US: d.dd or ddd
                                TRIM(REGEXP_REPLACE(amount, '[^0-9.-]+', '', 'g'))
                        END
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
