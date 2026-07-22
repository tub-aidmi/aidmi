{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN UPPER(stagename) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(stagename) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(stagename) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(stagename) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(stagename) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(stagename) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(stagename) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(stagename) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(stagename) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(stagename) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate -- YYYY-MM-DD
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g') IS NULL OR REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g') = '' THEN NULL
        WHEN POSITION(',' IN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g')) > POSITION('.' IN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g')) AND POSITION('.' IN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g')) > 0 THEN -- European dot-comma: 1.234,56
            CASE WHEN REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), '.', ''), ',', '.') ~ '^-?\d+(\.\d+)?$'
                 THEN REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                 ELSE NULL END
        WHEN POSITION(',' IN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g')) > 0 AND POSITION('.' IN REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g')) = 0 THEN -- European comma decimal: 123,45
            CASE WHEN REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), ',', '.') ~ '^-?\d+(\.\d+)?$'
                 THEN REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), ',', '.')::DOUBLE PRECISION
                 ELSE NULL END
        ELSE -- American or standard: 1,234.56 or 1234.56
            CASE WHEN REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), ',', '') ~ '^-?\d+(\.\d+)?$'
                 THEN REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), '£', ''), '\s', '', 'g'), ',', '')::DOUBLE PRECISION
                 ELSE NULL END
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}