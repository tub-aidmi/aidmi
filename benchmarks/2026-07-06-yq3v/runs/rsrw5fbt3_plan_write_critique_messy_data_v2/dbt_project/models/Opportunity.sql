-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    TRIM(opportunity.id) AS "Id",
    COALESCE(TRIM(opportunity.name), 'Unknown Opportunity') AS "Name",
    CASE UPPER(TRIM(opportunity.stagename))
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
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback for NOT NULL target
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN TRIM(opportunity.amount) IS NULL OR TRIM(opportunity.amount) = '' THEN NULL
            ELSE
                -- Step 1: Remove currency symbols and spaces from the amount string
                CASE
                    WHEN POSITION(',' IN REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')) > 0
                         AND POSITION('.' IN REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')) > 0 THEN
                        -- Contains both comma and dot. Determine format based on their relative positions.
                        CASE
                            WHEN POSITION(',' IN REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')) < POSITION('.' IN REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')) THEN
                                -- European format: dot is thousands separator, comma is decimal (e.g., 1.234,56)
                                REPLACE(
                                    REPLACE(
                                        REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g'),
                                        '.', ''
                                    ),
                                    ',', '.'
                                )
                            ELSE
                                -- US format: comma is thousands separator, dot is decimal (e.g., 1,234.56)
                                REPLACE(
                                    REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g'),
                                    ',', ''
                                )
                        END
                    WHEN POSITION(',' IN REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')) > 0 THEN
                        -- Contains only comma, assume European decimal (e.g., 123,45)
                        REPLACE(
                            REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g'),
                            ',', '.'
                        )
                    ELSE
                        -- Contains only dot (US decimal: 123.45) or no separators.
                        REGEXP_REPLACE(TRIM(opportunity.amount), '[€$ ]', '', 'g')
                END
        END AS DOUBLE PRECISION
    ) AS "Amount",
    TRIM(opportunity.currencyisocode) AS "CurrencyIsoCode",
    TRIM(opportunity.accountid) AS "AccountId",
    TRIM(opportunity.id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity