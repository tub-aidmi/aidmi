{{ config(materialized='table') }}

SELECT
    TRIM(o.id) AS "Id",
    INITCAP(TRIM(COALESCE(o.name, 'Unknown'))) AS "Name",
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
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN TRIM(o.closedate) IS NULL OR TRIM(o.closedate) = '' THEN NULL
        WHEN TRIM(o.closedate) ~ '^\d{4}[-/]\d{2}[-/]\d{2}$' THEN TO_DATE(TRIM(o.closedate), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(o.closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(o.closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(o.closedate) ~ '^\d{8}$' THEN TO_DATE(TRIM(o.closedate), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE
            CASE
                -- Step 1: Strip currency symbols and non-numeric characters, keep digits/dots/commas/signs
                WHEN REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g') = '' THEN NULL
                ELSE
                    CASE
                        -- European format with dots as thousands and comma as decimal: e.g. "1.234,56"
                        WHEN REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g') ~ '\.\d{3}(?=,)|,\d{2}$' THEN
                            CAST(
                                REPLACE(
                                    REPLACE(REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g'), '.', ''),
                                    ',', '.'
                                ) AS DOUBLE PRECISION
                            )
                        -- European format with comma only: e.g. "1234,56"
                        WHEN REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g') ~ ',\d{2}$' 
                             AND NOT REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g') ~ '\.' THEN
                            CAST(
                                REPLACE(REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g'), ',', '.') AS DOUBLE PRECISION
                            )
                        -- Standard format: remove thousand-separator commas, then parse
                        ELSE
                            CAST(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(TRIM(o.amount), '[^0-9.,\-+]', '', 'g'),
                                    '(?<=\d),(?=\d{3}(?:,|$))', ''
                                ) AS DOUBLE PRECISION
                            )
                    END
            END
    END AS "Amount",
    UPPER(TRIM(COALESCE(o.currencyisocode, 'USD'))) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    TRIM(o.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
    ON TRIM(o.accountid) = TRIM(a.id)