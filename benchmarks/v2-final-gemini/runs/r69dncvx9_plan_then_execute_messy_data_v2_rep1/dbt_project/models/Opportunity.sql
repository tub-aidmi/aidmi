-- depends_on: {{ ref('opportunity') }}
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(INITCAP(o.name)), 'Unknown') AS "Name",
    CASE TRIM(LOWER(o.stagename))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default as text for NOT NULL
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN TRIM(o.amount) IS NULL OR TRIM(o.amount) = '' THEN NULL
            ELSE
                -- Clean the string first: remove all non-numeric, non-dot, non-comma, non-minus characters
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,-]+', '', 'g'),
                    ',', '@TEMP_COMMA@'), -- Temporarily replace comma with a unique placeholder
                '.', ''), -- Remove all dots (assumed thousands separators if comma is present)
            '@TEMP_COMMA@', '.' -- Replace the placeholder comma with a dot (decimal separator)
        END AS DOUBLE PRECISION
    ) AS "Amount",
    UPPER(TRIM(o.currencyisocode)) AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o