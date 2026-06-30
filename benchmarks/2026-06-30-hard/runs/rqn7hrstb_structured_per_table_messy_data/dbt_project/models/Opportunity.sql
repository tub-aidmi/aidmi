normalize_text = (col) => trim(lower(col)) FROM (
{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    COALESCE(TRIM(src."Name"), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(src."StageName")) IN ('prospect', 'prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN TRIM(LOWER(src."StageName")) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN TRIM(LOWER(src."StageName")) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(src."StageName")) IN ('value proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(src."StageName")) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(src."StageName")) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(src."StageName")) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(src."StageName")) IN ('negotiation/review', 'in prüfung') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(src."StageName")) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
        WHEN TRIM(LOWER(src."StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL constraint
    END AS "StageName",
    COALESCE(
        -- YYYY-MM-DD
        (CASE WHEN TRIM(src."CloseDate") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(src."CloseDate"), 'YYYY-MM-DD'), 'YYYY-MM-DD') END),
        -- DD.MM.YYYY
        (CASE WHEN TRIM(src."CloseDate") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src."CloseDate"), 'DD.MM.YYYY'), 'YYYY-MM-DD') END),
        -- YYYYMMDD
        (CASE WHEN TRIM(src."CloseDate") ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(src."CloseDate"), 'YYYYMMDD'), 'YYYY-MM-DD') END),
        -- M/D/YYYY or MM/DD/YYYY
        (CASE WHEN TRIM(src."CloseDate") ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(src."CloseDate"), 'FMM/FMD/YYYY'), 'YYYY-MM-DD') END),
        '1900-01-01' -- Default value for unparseable or NULL dates to satisfy NOT NULL constraint
    ) AS "CloseDate",
    CASE
        WHEN TRIM(src."Amount") IS NULL OR TRIM(src."Amount") IN ('None', '') THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN TRIM(REGEXP_REPLACE(src."Amount", 'EUR ', '', 'i')) ~ '^[0-9.]+,[0-9]+$' THEN -- European format (e.g., 1.234,56)
                        REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(src."Amount", 'EUR ', '', 'i')), '\\.', '', 'g'), ',', '.')
                    WHEN TRIM(REGEXP_REPLACE(src."Amount", 'EUR ', '', 'i')) ~ '^-?[0-9]*\\.?[0-9]+$' THEN -- US format or simple number (e.g., 1234.56)
                        TRIM(REGEXP_REPLACE(src."Amount", 'EUR ', '', 'i'))
                    ELSE NULL
                END AS DOUBLE PRECISION
            )
    END AS "Amount",
    src."CurrencyIsoCode" AS "CurrencyIsoCode",
    src."AccountId" AS "AccountId",
    src."Id" AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }} src