{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Opportunity') AS "Name",
    CASE UPPER(TRIM(src.stagename))
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
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            CASE
                WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(src.closedate, 'YYYY-MM-DD')
                WHEN src.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(src.closedate, 'MM/DD/YYYY')
                WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(src.closedate, 'DD.MM.YYYY')
                ELSE NULL
            END,
            CURRENT_DATE
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    CASE
        WHEN TRIM(src.amount) IS NULL OR TRIM(src.amount) = '' THEN NULL
        ELSE
            -- Use a subquery to clean the string once and then process it
            (SELECT
                CAST(
                    CASE
                        WHEN POSITION(',', cleaned_str) > 0 AND POSITION('.', cleaned_str) > 0 THEN
                            -- Both dot and comma present. Apply European rule if dot before comma.
                            CASE
                                WHEN POSITION('.' IN cleaned_str) < POSITION(',', cleaned_str) THEN
                                    REPLACE(REPLACE(cleaned_str, '.', ''), ',', '.')
                                ELSE -- US style
                                    REPLACE(cleaned_str, ',', '')
                            END
                        WHEN POSITION(',', cleaned_str) > 0 THEN
                            -- Only comma present. Assume European decimal separator.
                            REPLACE(cleaned_str, ',', '.')
                        ELSE
                            -- Only dot present or no separators. Assume standard.
                            cleaned_str
                    END AS TEXT
                ) :: DOUBLE PRECISION
            FROM (SELECT REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,]+', '', 'g') AS cleaned_str) AS _cleaned_amount_subquery
            )
    END AS "Amount",
    src.currencyisocode AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src
