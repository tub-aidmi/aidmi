{{ config(materialized='table') }}

SELECT 
    "id" AS "Id",
    "name" AS "Name",
    CASE 
        WHEN LOWER(TRIM("stagename")) IN ('prospecting', 'prospect', 'in kontakt', 'in prüfung') THEN 'Prospecting'
        WHEN LOWER(TRIM("stagename")) IN ('qualification', 'quali', 'qualifikation', 'qualif') THEN 'Qualification'
        WHEN LOWER(TRIM("stagename")) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM("stagename")) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM("stagename")) IN ('id. decision makers', 'entscheider identifizieren') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM("stagename")) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM("stagename")) IN ('proposal/price quote', 'angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM("stagename")) IN ('negotiation/review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM("stagename")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM("stagename")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE 
        WHEN "closedate" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("closedate", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("closedate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "closedate" ~ '^\d{4}-\d{2}-\d{2}$' THEN "closedate"
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN "amount" ~ '^[0-9\-]+\.[0-9]+$' THEN CAST("amount" AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[0-9\-]+,[0-9]+$' THEN CAST(REPLACE("amount", ',', '.') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[0-9\-]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE("amount", '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[A-Za-z\s]+[0-9\-]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE("amount", '[^0-9\-.]', '', 'g') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[A-Za-z\s]+[0-9\-]+,[0-9]+$' THEN CAST(REPLACE(REGEXP_REPLACE("amount", '[^0-9\-,]', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN "amount" ~ '^[0-9\-]+$' THEN CAST("amount" AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN LOWER(TRIM("currencyisocode")) IN ('dollar', 'usd') THEN 'USD'
        WHEN LOWER(TRIM("currencyisocode")) IN ('euro', 'eur') THEN 'EUR'
        WHEN LOWER(TRIM("currencyisocode")) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM("currencyisocode")) IN ('£', 'gbp') THEN 'GBP'
        ELSE "currencyisocode"
    END AS "CurrencyIsoCode",
    "accountid" AS "AccountId",
    "id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}