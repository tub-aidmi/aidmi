{{ config(materialized='table') }}

SELECT
    CONCAT('006', LEFT(MD5(chance_id), 17)) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    CASE
        WHEN UPPER(TRIM(phase)) LIKE '%PROSPEKT%' THEN 'Prospecting'
        WHEN UPPER(TRIM(phase)) LIKE '%QUALIFIZIERUNG%' OR UPPER(TRIM(phase)) LIKE '%QUALIFICAT%' THEN 'Qualification'
        WHEN UPPER(TRIM(phase)) LIKE '%BEDARFS%' OR UPPER(TRIM(phase)) LIKE '%NEEDS%' OR UPPER(TRIM(phase)) LIKE '%ANALYSE%' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(phase)) LIKE '%WERT%' OR UPPER(TRIM(phase)) LIKE '%VALUE%' OR UPPER(TRIM(phase)) LIKE '%PROPOSITION%' THEN 'Value Proposition'
        WHEN UPPER(TRIM(phase)) LIKE '%ENTSCHEID%' OR UPPER(TRIM(phase)) LIKE '%DECISION%' OR UPPER(TRIM(phase)) LIKE '%MASTERS%' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(phase)) LIKE '%WAHRNEHMUNG%' OR UPPER(TRIM(phase)) LIKE '%PERCEPTION%' OR UPPER(TRIM(phase)) LIKE '%ANALYSE%' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(phase)) LIKE '%ANGEBOT%' OR UPPER(TRIM(phase)) LIKE '%PROPOSAL%' OR UPPER(TRIM(phase)) LIKE '%PREIS%' OR UPPER(TRIM(phase)) LIKE '%PRICE%' OR UPPER(TRIM(phase)) LIKE '%ZITAT%' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(phase)) LIKE '%VERHANDLUNG%' OR UPPER(TRIM(phase)) LIKE '%NEGOTIAT%' OR UPPER(TRIM(phase)) LIKE '%PRÜFUNG%' OR UPPER(TRIM(phase)) LIKE '%REVIEW%' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(phase)) LIKE '%ABSCHLUSS GEWON%' OR UPPER(TRIM(phase)) LIKE '%CLOSED WON%' OR UPPER(TRIM(phase)) LIKE '%GEWONNEN%' THEN 'Closed Won'
        WHEN UPPER(TRIM(phase)) LIKE '%ABSCHLUSS VERLOREN%' OR UPPER(TRIM(phase)) LIKE '%CLOSED LOST%' OR UPPER(TRIM(phase)) LIKE '%VERLOREN%' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(abschlussdatum, 'DD.MM.YYYY')::TEXT
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_DATE(abschlussdatum, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN waehrung IS NULL OR TRIM(waehrung) = '' THEN NULL
        WHEN NOT waehrung ~ '[0-9]' THEN NULL
        WHEN REGEXP_REPLACE(waehrung, '[^0-9.,-]', '', 'g') = '' THEN NULL
        WHEN REGEXP_REPLACE(waehrung, '[^0-9.,-]', '', 'g') ~ '^[-,.]+$' THEN NULL
        WHEN waehrung ~ '\.' AND waehrung ~ ',' THEN
            CASE
                WHEN POSITION(',' IN waehrung) > POSITION('.' IN waehrung) THEN
                    -- European format: dot is thousands, comma is decimal
                    CAST(
                        REPLACE(REGEXP_REPLACE(waehrung, '[^0-9.,-]', '', 'g'), '.', '') || '.' || REVERSE(SPLIT_PART(REVERSE(TRIM(waehrung)), ',', 1))
                    AS DOUBLE PRECISION)
                ELSE NULL
            END
        WHEN waehrung ~ ',' AND NOT waehrung ~ '\.' THEN
            CASE
                WHEN POSITION(',' IN waehrung) > POSITION('.' IN waehrung) THEN
                    -- Comma as thousands separator (US format)
                    CAST(REPLACE(waehrung, ',', '') AS DOUBLE PRECISION)
                ELSE
                    -- Comma as decimal separator
                    CAST(REPLACE(waehrung, ',', '.') AS DOUBLE PRECISION)
            END
        WHEN waehrung ~ '\.' AND NOT waehrung ~ ',' THEN
            CASE
                -- Check if dot is thousands separator (e.g. "1.234") vs decimal (e.g. "1.5")
                WHEN LENGTH(SPLIT_PART(waehrung, '.', 2)) = 3 AND NOT LENGTH(SPLIT_PART(waehrung, '.', 1)) BETWEEN 0 AND 1 THEN
                    CAST(REPLACE(waehrung, '.', '') AS DOUBLE PRECISION)
                ELSE
                    CAST(REGEXP_REPLACE(waehrung, '[^0-9.,-]', '', 'g') AS DOUBLE PRECISION)
            END
        ELSE
            CASE
                WHEN waehrung ~ ',' THEN CAST(REPLACE(waehrung, ',', '.') AS DOUBLE PRECISION)
                ELSE CAST(REGEXP_REPLACE(waehrung, '[^0-9.,-]', '', 'g') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(waehrung)) IN ('USD', 'EURO', 'EUR', '€', '$') THEN 'USD'
        ELSE LEFT(UPPER(TRIM(waehrung)), 3)
    END AS "CurrencyIsoCode",
    CONCAT('001', LEFT(MD5(kd_nr), 17)) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
