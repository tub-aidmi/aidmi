{{ config(materialized='table') }}

SELECT
    CAST("Id" AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM("Name")), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM("StageName")) IN ('prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM("StageName")) IN ('quali', 'qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM("StageName")) = 'in prüfung' THEN 'Needs Analysis'
        WHEN LOWER(TRIM("StageName")) = 'wertversprechen' THEN 'Value Proposition'
        WHEN LOWER(TRIM("StageName")) IN ('id. decision makers', 'identifizierung von entscheidern') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM("StageName")) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM("StageName")) IN ('angebot/preisanfrage', 'angebot', 'proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM("StageName")) IN ('verhandlung', 'prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM("StageName")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM("StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM("StageName")) = 'in kontakt' THEN 'Prospecting'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN TRIM(COALESCE("CloseDate", '')) = '' OR UPPER(TRIM(COALESCE("CloseDate", ''))) IN ('NULL', 'N/A') THEN NULL
        WHEN TRIM(COALESCE("CloseDate", '')) = '0000-00-00' THEN NULL
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(COALESCE("CloseDate", '')), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(COALESCE("CloseDate", '')), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(COALESCE("CloseDate", '')), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{8}$' THEN TO_DATE(TRIM(COALESCE("CloseDate", '')), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN LOWER(TRIM(COALESCE("Amount", ''))) IN ('', 'null', 'none') THEN NULL
        ELSE
            CASE
                WHEN "Amount" ~ '\d{1,3}\.\d{3},\d+' THEN
                    CAST(REPLACE(
                        REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE("Amount", '[€$£]', '', 'g'), 'EUR\s*', '', 'i'),
                         '.', ''), ',', '.') AS DOUBLE PRECISION)
                ELSE
                    CAST(REGEXP_REPLACE(REGEXP_REPLACE("Amount", '[€$£]', '', 'g'), 'EUR\s*', '', 'i') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('eur', '€') THEN 'EUR'
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('usd', 'dollar', '$') THEN 'USD'
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) = 'chf' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CAST("AccountId" AS TEXT) AS "AccountId",
    CASE
        WHEN "Id" IS NOT NULL THEN 'OPP-' || LTRIM(SPLIT_PART(CAST("Id" AS TEXT), '-', 2), '0')::TEXT
        ELSE NULL
    END AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}