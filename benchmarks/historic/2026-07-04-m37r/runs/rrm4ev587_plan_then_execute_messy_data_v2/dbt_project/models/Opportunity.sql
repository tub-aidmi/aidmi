{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Opportunity') AS "Name",
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
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        -- YYYY-MM-DD format
        (CASE WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END),
        -- DD.MM.YYYY format
        (CASE WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        -- YYYYMMDD format
        (CASE WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END),
        -- M/D/YYYY or MM/DD/YYYY format
        (CASE WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        -- Fallback for unparseable dates, since CloseDate is NOT NULL
        '1900-01-01'
    ) AS "CloseDate",
    COALESCE(
        NULLIF(REGEXP_REPLACE(REPLACE(REPLACE(TRIM(amount), '.', ''), ',', '.'), '[^0-9.-]', '', 'g'), ''),
        NULL
    )::DOUBLE PRECISION AS "Amount",
    TRIM(UPPER(currencyisocode)) AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
