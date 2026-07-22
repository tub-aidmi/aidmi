{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE("name", 'Unknown') AS "Name",
    CASE 
        WHEN UPPER(TRIM("stagename")) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM("stagename")) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM("stagename")) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM("stagename")) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM("stagename")) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("stagename")) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM("stagename")) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("stagename")) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("stagename")) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM("stagename")) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM("stagename")) IN ('IN PRÜFUNG', 'IN KONTAKT') THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN "closedate" ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("closedate", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{4}-\d{2}-\d{2}$' THEN "closedate"
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN "amount" IS NULL OR TRIM("amount") IN ('', 'None', 'N/A') THEN NULL
        ELSE 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE("amount", '[^\d,.]', '', 'g'),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) AS DOUBLE PRECISION
            )
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM("currencyisocode")) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM("currencyisocode")) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM("currencyisocode")) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM("currencyisocode")) IN ('GBP', '£') THEN 'GBP'
        ELSE "currencyisocode"
    END AS "CurrencyIsoCode",
    "accountid" AS "AccountId",
    "id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
