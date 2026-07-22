{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(NULLIF(TRIM("Name"), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM("StageName")) IN ('prospect', 'prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM("StageName")) IN ('quali', 'qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM("StageName")) = 'in prüfung' THEN 'Needs Analysis'
        WHEN LOWER(TRIM("StageName")) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM("StageName")) IN ('id. decision makers', 'identifizierung entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM("StageName")) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM("StageName")) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM("StageName")) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM("StageName")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM("StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Closed Lost'
    END AS "StageName",
    CASE
        WHEN "CloseDate" IS NULL OR TRIM("CloseDate") = '' OR UPPER(TRIM("CloseDate")) IN ('N/A', '-0', '-', 'NULL', 'NA') OR TRIM("CloseDate") = '0000-00-00' THEN NULL
        WHEN "CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM("CloseDate"), 'YYYY-MM-DD')::TEXT
        WHEN "CloseDate" ~ '^\d{8}$' THEN TO_DATE(TRIM("CloseDate"), 'YYYYMMDD')::TEXT
        WHEN "CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM("CloseDate"), 'MM/DD/YYYY')::TEXT
        WHEN "CloseDate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM("CloseDate"), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN "Amount" IS NULL OR TRIM("Amount") = '' THEN NULL
        WHEN UPPER(TRIM("Amount")) IN ('NONE', 'N/A', '-', 'NULL', 'NA', '0', '-0') THEN 0.0
        ELSE CAST(REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE("Amount", '[^\d.,\-]', '', 'g'),
                '\.', '', 'g'
            ),
            ',', '.', 'g'
        ) AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN "CurrencyIsoCode" IS NULL THEN NULL
        WHEN UPPER(TRIM("CurrencyIsoCode")) IN ('EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM("CurrencyIsoCode")) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM("CurrencyIsoCode")) = 'CHF' THEN 'CHF'
        WHEN UPPER(TRIM("CurrencyIsoCode")) = 'GBP' THEN 'GBP'
        ELSE UPPER(TRIM("CurrencyIsoCode"))
    END AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    "Id" AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
      0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}