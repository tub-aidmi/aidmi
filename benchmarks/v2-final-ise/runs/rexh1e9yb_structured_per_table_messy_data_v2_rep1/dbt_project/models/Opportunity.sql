{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) = 'in kontakt' THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALI', 'QUALIFICATION', 'QUALIFIKATION') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'in prüfung' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS', 'IDENTIFYING DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSMUSTER', 'PERZEPTIONSANALYSE') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSION / PREISANGABE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG / PRÜFUNG') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'LOST') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'M/D/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' OR LOWER(TRIM(amount)) = 'none' THEN NULL
        ELSE CAST(
            CASE
                -- European format: dot before comma (e.g., "60.702,05")
                WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g') LIKE '%.%'
                     AND REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g') LIKE '%,%'
                     AND POSITION('.' IN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g')) <
                         POSITION(',' IN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '', 'g')) THEN
                    -- Remove thousands dots, replace decimal comma with dot
                    CAST(REPLACE(
                        REPLACE(
                            REGEXP_REPLACE(TRIM(amount), 'EUR\s*', '', 'gi')
                        , '.', ''  -- strip thousands dots
                        ), ',' , '.'  -- convert decimal comma to point
                    AS DOUBLE PRECISION))
                ELSE
                    -- US format or plain: remove everything except digits, dots, minus sign
                    CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
            END
        )
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(currencyisocode)) IN ('USD', 'DOLLAR', '$', '€') THEN 'USD'
        WHEN UPPER(TRIM(currencyisocode)) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', 'POUND', '£') THEN 'GBP'
        WHEN UPPER(TRIM(currencyisocode)) = 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}