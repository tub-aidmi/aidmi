{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) IN ('NEEDS ANALYSIS', 'NEEDS_ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('VALUE PROPOSITION', 'VALUE_PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS', 'ID_DECISION_MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) IN ('PERCEPTION ANALYSIS', 'PERCEPTION_ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL_PRICE_QUOTE', 'PROPOSAL/PRICEQUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION_REVIEW', 'NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'CLOSED_WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'CLOSED_LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN amount ~ '^[\d\s.,-]+$' THEN 
            CASE 
                WHEN amount ~ '^\d{1,3}(\.\d{3})*,\d+$' THEN 
                    -- European format: 1.234,56 -> 1234.56
                    REGEXP_REPLACE(REGEXP_REPLACE(amount, '[\s.]', '', 'g'), ',', '.', 'g')::DOUBLE PRECISION
                WHEN amount ~ ',' AND NOT amount ~ '.' THEN 
                    -- Simple comma decimal: 1234,56 -> 1234.56
                    REGEXP_REPLACE(amount, '[\s,]', '', 'g')::DOUBLE PRECISION / 100.0
                ELSE 
                    -- Standard format: 1,234.56 or 1234.56
                    REGEXP_REPLACE(amount, '[\s,]', '', 'g')::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(currencyisocode), '') AS "CurrencyIsoCode",
    accountid AS "AccountId",
    NULLIF(TRIM(id), '') AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
