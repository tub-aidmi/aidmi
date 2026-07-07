{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    COALESCE(TRIM(name), 'N/A') AS "Name",
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
        ELSE 'Prospecting' -- Default to satisfy NOT NULL constraint
    END AS "StageName",
    COALESCE(
        CASE
            WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default to satisfy NOT NULL constraint
    ) AS "CloseDate",
    -- Remove currency symbols and commas, then cast to DOUBLE PRECISION
    NULLIF(CAST(REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), ',', '') AS DOUBLE PRECISION), NULL) AS "Amount",
    TRIM(currencyisocode) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
