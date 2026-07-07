-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    TRIM(source.id) AS "Id",
    COALESCE(TRIM(source.name), 'Unknown Opportunity ' || TRIM(source.id)) AS "Name",
    CASE UPPER(TRIM(source.stagename))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        (CASE WHEN TRIM(source.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(source.closedate), 'YYYY-MM-DD')::TEXT END),
        (CASE WHEN TRIM(source.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(source.closedate), 'DD.MM.YYYY')::TEXT END),
        (CASE WHEN TRIM(source.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(source.closedate), 'MM/DD/YYYY')::TEXT END),
        (CASE WHEN TRIM(source.closedate) ~ '^\d{8}$' THEN TO_DATE(TRIM(source.closedate), 'YYYYMMDD')::TEXT END),
        CURRENT_DATE
    ) AS "CloseDate",
    NULLIF(
        CASE
            WHEN TRIM(source.amount) IS NULL OR TRIM(source.amount) = '' THEN NULL
            ELSE
                -- Remove currency symbols and spaces
                REGEXP_REPLACE(
                    -- Handle European format (e.g., 1.234,56 -> 1234.56)
                    REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(source.amount), '[€$£]', '', 'g'),
                            -- Remove dots that are thousand separators (followed by 3 digits and a comma)
                            '\.(?=\d{3},)', '', 'g'
                        ),
                        -- Swap comma to decimal point if present
                        ',', '.'
                    ),
                    -- Remove any remaining commas (for US format e.g., 1,234.56 -> 1234.56)
                    ',', ''
                )
        END,
        ''
    )::DOUBLE PRECISION AS "Amount",
    TRIM(source.currencyisocode) AS "CurrencyIsoCode",
    TRIM(source.accountid) AS "AccountId",
    TRIM(source.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE AS "CreatedDate",
    CURRENT_DATE AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS source