-- {{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(src.stagename)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(src.stagename)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(src.stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(src.stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(src.stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(src.stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(src.stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(src.stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(src.stagename)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(src.stagename)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END AS "StageName",
    COALESCE(
        CASE
            WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN src.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN src.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(src.amount, '[^0-9.,]+(?<!\d\.|,\d)', '', 'g'), ',', '.')) ~ '^-?\d+(\.\d+)?$'
        THEN CAST(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(src.amount, '[^0-9.,]+(?<!\d\.|,\d)', '', 'g'), ',', '.')) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    src.currencyisocode AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src