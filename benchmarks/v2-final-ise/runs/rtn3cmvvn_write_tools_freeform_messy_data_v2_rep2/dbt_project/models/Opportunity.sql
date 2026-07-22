{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION', 'IN PRÜFUNG', 'IN KONTAKT') THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount ~ '^[\d]+[.,\d]+$' THEN
            CASE
                WHEN amount ~ ',' THEN
                    CASE
                        WHEN amount ~ '\.' THEN
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(amount, '\.', '', 'g'),
                                ',',
                                '.',
                                'g'
                            )::DOUBLE PRECISION
                        ELSE
                            REGEXP_REPLACE(amount, ',', '.', 'g')::DOUBLE PRECISION
                    END
                ELSE
                    amount::DOUBLE PRECISION
            END
        WHEN amount ~ '^[A-Za-z\s€£$]+[\d]+[.,\d]*$' THEN
            REGEXP_REPLACE(
                REGEXP_REPLACE(amount, '[^\d.,]', '', 'g'),
                ',',
                '.',
                'g'
            )::DOUBLE PRECISION
        WHEN amount ~ '^[\d]+$' THEN amount::DOUBLE PRECISION
        WHEN amount ~ '^-?[\d]+[.,\d]+$' THEN
            CASE
                WHEN amount ~ ',' THEN
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(amount, '\.', '', 'g'),
                        ',',
                        '.',
                        'g'
                    )::DOUBLE PRECISION
                ELSE
                    amount::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('USD', 'DOLLAR', '$', 'US DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(currencyisocode)) IN ('CHF', 'SWISS FRANC', 'SFR') THEN 'CHF'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', 'POUND', '£') THEN 'GBP'
        ELSE currencyisocode
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
