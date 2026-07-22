{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(TRIM("name"), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM("stagename")) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT', 'IN CONTACT') THEN 'Prospecting'
        WHEN UPPER(TRIM("stagename")) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM("stagename")) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM("stagename")) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM("stagename")) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("stagename")) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM("stagename")) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'PRICE QUOTE', 'IN PRÜFUNG', 'IN REVIEW') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("stagename")) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION', 'REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("stagename")) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM("stagename")) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN', 'LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN NULLIF(TRIM("closedate"), '') IS NULL THEN NULL
        WHEN "closedate" ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE("closedate", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{4}-\d{2}-\d{2}$' THEN "closedate"
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN NULLIF(TRIM("amount"), '') IS NULL THEN NULL
        WHEN "amount" ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE("amount", '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE("amount", ',', '.') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[A-Za-z€£$]+[[:space:]]*[0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE("amount", '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[A-Za-z€£$]+[[:space:]]*[0-9]+$' THEN CAST(REGEXP_REPLACE("amount", '[^0-9]', '', 'g') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[+-]?[0-9]+\.[0-9]+$' THEN CAST("amount" AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[+-]?[0-9]+$' THEN CAST("amount" AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN NULLIF(TRIM("currencyisocode"), '') IS NULL THEN NULL
        WHEN UPPER(TRIM("currencyisocode")) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM("currencyisocode")) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM("currencyisocode")) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM("currencyisocode")) IN ('CHF') THEN 'CHF'
        ELSE "currencyisocode"
    END AS "CurrencyIsoCode",
    "accountid" AS "AccountId",
    "id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
