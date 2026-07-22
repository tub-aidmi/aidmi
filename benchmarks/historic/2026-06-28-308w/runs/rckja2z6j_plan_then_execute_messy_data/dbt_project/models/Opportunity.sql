'''{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(UPPER("StageName")) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN TRIM(UPPER("StageName")) IN ('QUALIFIKATION', 'QUALIFICATION', 'QUALI') THEN 'Qualification'
        WHEN TRIM(UPPER("StageName")) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN TRIM(UPPER("StageName")) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN TRIM(UPPER("StageName")) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN TRIM(UPPER("StageName")) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN TRIM(UPPER("StageName")) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN TRIM(UPPER("StageName")) IN ('NEGOTIATION/REVIEW', 'IN PRÜFUNG') THEN 'Negotiation/Review'
        WHEN TRIM(UPPER("StageName")) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN TRIM(UPPER("StageName")) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Fallback for NOT NULL enum
    END AS "StageName",
    COALESCE(
        CASE
            WHEN "CloseDate" ~ '^''\'''d{4}-''\'''d{2}-''\'''d{2}$' THEN TO_CHAR(CAST("CloseDate" AS DATE), 'YYYY-MM-DD')
            WHEN "CloseDate" ~ '^''\'''d{8}$' THEN TO_CHAR(TO_DATE("CloseDate", 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN "CloseDate" ~ '^''\'''d{1,2}/''\'''d{1,2}/''\'''d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN "CloseDate" ~ '^''\'''d{1,2}\'.''\'''d{1,2}\'.''\'''d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Fallback for NOT NULL date
    ) AS "CloseDate",
    CASE
        WHEN "Amount" IS NULL OR TRIM("Amount") = '' THEN NULL
        ELSE
            CAST(
                CASE
                    -- If it contains both a comma and a dot, check their relative positions to determine format.
                    WHEN POSITION(',' IN REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g')) > 0
                         AND POSITION('.' IN REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g')) > 0 THEN
                        CASE
                            -- Comma appears after the dot (e.g., '1.234,56') -> European format
                            WHEN POSITION(',' IN REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g')) > POSITION('.' IN REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g')) THEN
                                REPLACE(REPLACE(REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g'), '.', ''), ',', '.')
                            -- Dot appears after the comma (e.g., '1,234.56') -> US/Standard format
                            ELSE
                                REPLACE(REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g'), ',', '')
                        END
                    -- Only one type of separator or no separators
                    WHEN POSITION(',' IN REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g')) > 0 THEN
                        -- If only comma, assume it's a decimal separator (e.g., '123,45')
                        REPLACE(REGEXP_REPLACE(TRIM("Amount"), '[^0-9.,-]', '', 'g'), ',', '.')
                    ELSE
                        -- If only dot or no separators, assume standard (e.g., '123.45' or '12345')
                        REGEXP_REPLACE(TRIM("Amount"), '[^0-9.-]', '', 'g')
                END AS DOUBLE PRECISION
            )
    END AS "Amount",
    "CurrencyIsoCode" AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    NULL::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }}'''