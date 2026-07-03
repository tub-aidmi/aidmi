{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ source('fixture_messy_data_src', 'Opportunity') }}
),

cleaned AS (
    SELECT
        -- Id: keep as-is, NOT NULL
        CAST("Id" AS TEXT) AS "Id",
        
        -- Name: trim, default to empty string if NULL (NOT NULL in target)
        COALESCE(TRIM("Name"), '') AS "Name",
        
        -- StageName: normalize to enum domain (NOT NULL in target, default to Prospecting)
        COALESCE(
            CASE
                WHEN LOWER(TRIM("StageName")) IN ('prospect', 'prospecting') THEN 'Prospecting'
                WHEN LOWER(TRIM("StageName")) IN ('quali', 'qualification', 'qualifikation') THEN 'Qualification'
                WHEN LOWER(TRIM("StageName")) IN ('needs analysis', 'in prüfung') THEN 'Needs Analysis'
                WHEN LOWER(TRIM("StageName")) IN ('value proposition', 'wertversprechen') THEN 'Value Proposition'
                WHEN LOWER(TRIM("StageName")) IN ('id. decision makers', 'identifizierung von entscheidern') THEN 'Id. Decision Makers'
                WHEN LOWER(TRIM("StageName")) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
                WHEN LOWER(TRIM("StageName")) IN ('proposal/price quote', 'angebot/preisanfrage', 'angebot') THEN 'Proposal/Price Quote'
                WHEN LOWER(TRIM("StageName")) IN ('negotiation/review', 'verhandlung', 'prüfung') THEN 'Negotiation/Review'
                WHEN LOWER(TRIM("StageName")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
                WHEN LOWER(TRIM("StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
                WHEN LOWER(TRIM("StageName")) = 'in kontakt' THEN 'Prospecting'
                ELSE 'Prospecting'
            END,
            'Prospecting'
        ) AS "StageName",
        
        -- CloseDate: parse multiple formats, return ISO YYYY-MM-DD (NOT NULL in target)
        COALESCE(
            CASE
                WHEN "CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' AND "CloseDate" != '0000-00-00'
                    THEN TO_DATE("CloseDate", 'YYYY-MM-DD')::TEXT
                WHEN "CloseDate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
                    THEN TO_DATE("CloseDate", 'DD.MM.YYYY')::TEXT
                WHEN "CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                    THEN TO_DATE("CloseDate", 'MM/DD/YYYY')::TEXT
                WHEN "CloseDate" ~ '^\d{8}$'
                    THEN TO_DATE("CloseDate", 'YYYYMMDD')::TEXT
                ELSE NULL
            END,
            '1970-01-01'
        ) AS "CloseDate",
        
        -- Amount: strip currency symbols/prefixes, handle European format, convert to DOUBLE PRECISION
        CASE
            WHEN TRIM("Amount") = '' OR LOWER(TRIM("Amount")) = 'none' THEN NULL
            ELSE
                CASE
                    -- European format: digits.digits,digits (e.g., 1.234,56 or 404.415,29)
                    WHEN REGEXP_REPLACE(LOWER(TRIM("Amount")), '[^0-9.,]+', '') ~ '^\-?\d{1,3}(\.\d{3})+(\,\d+)?$'
                        THEN REPLACE(REPLACE(
                            REGEXP_REPLACE(LOWER(TRIM("Amount")), '[^0-9.,]+', ''),
                            '.', ''), ',', '.')::DOUBLE PRECISION
                    -- Standard format: plain decimal (e.g., 1234.56 or -1234.56)
                    WHEN REGEXP_REPLACE(LOWER(TRIM("Amount")), '[^0-9.,]+', '') ~ '^\-?\d+(\.\d+)?$'
                        THEN REGEXP_REPLACE(LOWER(TRIM("Amount")), '[^0-9.,]+', '')::DOUBLE PRECISION
                    ELSE NULL
                END
        END AS "Amount",
        
        -- CurrencyIsoCode: normalize to standard ISO codes
        CASE
            WHEN LOWER(TRIM("CurrencyIsoCode")) IN ('eur', '€') THEN 'EUR'
            WHEN LOWER(TRIM("CurrencyIsoCode")) IN ('gbp', '£') THEN 'GBP'
            WHEN LOWER(TRIM("CurrencyIsoCode")) IN ('usd', 'dollar', '$') THEN 'USD'
            WHEN LOWER(TRIM("CurrencyIsoCode")) = 'chf' THEN 'CHF'
            ELSE UPPER(TRIM("CurrencyIsoCode"))
        END AS "CurrencyIsoCode",
        
        -- AccountId: keep as-is (foreign key to Account)
        CAST("AccountId" AS TEXT) AS "AccountId"
        
    FROM src
)

SELECT
    c."Id",
    c."Name",
    c."StageName",
    c."CloseDate",
    c."Amount",
    c."CurrencyIsoCode",
    c."AccountId",
    
    -- Legacy fields (derived or static)
    CAST('OPP-' || LTRIM(SPLIT_PART(c."Id", '-', 2), '0') AS TEXT) AS "Legacy_Opportunity_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
    
FROM cleaned c
