{{ config(materialized='table') }}

WITH opp_raw AS (
    SELECT
        -- Opportunity ID and legacy key
        opp_kennung AS "Id",
        opp_kennung AS "Legacy_Opportunity_ID__c",

        -- Name from title, fallback to Id if empty
        COALESCE(NULLIF(TRIM(titel), ''), opp_kennung) AS "Name",

        -- Stage name mapping (case-insensitive with German/English variants)
        CASE
            WHEN LOWER(TRIM(vertriebsphase)) IN ('in kontakt', 'prospect', 'prospecting')
                THEN 'Prospecting'
            WHEN LOWER(TRIEBSPHASE) IN ('qualification', 'quali', 'qualifikation', 'in prüfung')
                THEN 'Qualification'
            WHEN LOWER(TRIM(vertriebsphase)) LIKE '%gewonnen%' OR LOWER(TRIM(vertriebsphase)) = 'won'
                THEN 'Closed Won'
            WHEN LOWER(TRIM(vertriebsphase)) LIKE '%verloren%' OR LOWER(TRIM(vertriebsphase)) LIKE '%lost%'
                THEN 'Closed Lost'
            ELSE 'Prospecting'  -- default fallback
        END AS "StageName",

        -- Close date: parse multiple formats → ISO YYYY-MM-DD
        CASE
            WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TRIM(zieldatum)
            WHEN TRIM(zieldatum) ~ '^\d{8}$'
                THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
                THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "CloseDate",

        -- Amount: strip currency prefixes/symbols, handle European dot-comma format
        CASE
            WHEN TRIM(UPPER(auftragswert)) = 'NONE' OR TRIM(auftragswert) IS NULL THEN NULL
            ELSE
                CAST(
                    CASE
                        -- European format: pattern like "159.961,05" (dots then comma as decimal)
                        WHEN REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,-]', '') ~ '[\d]\.[\d]{3},[\d]'
                        THEN REGEXP_REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[^\d.,-]', ''), '\.', ''),
                            ',', '.')::DOUBLE PRECISION
                        -- Standard format: strip currency prefix/symbols and cast
                        ELSE REGEXP_REPLACE(TRIM(auftragswert), '^(EUR|USD|GBP|CHF|€|\$|%|\£)\s*', '')::DOUBLE PRECISION
                    END
                )
        END AS "Amount",

        -- Currency code mapping (case-insensitive, symbol normalization)
        CASE
            WHEN UPPER(TRIM(waehrungscode)) IN ('CHF', 'CH') THEN 'CHF'
            WHEN UPPER(TRIM(waehrungscode)) IN ('EUR', 'EURO') OR waehrungscode = '€' THEN 'EUR'
            WHEN UPPER(TRIM(waehrungscode)) IN ('GBP', 'POUND') OR waehrungscode IN ('£', 'GBR') THEN 'GBP'
            WHEN UPPER(TRIM(waehrungscode)) IN ('USD', 'DOLLAR', 'US$', '$') THEN 'USD'
            ELSE NULL
        END AS "CurrencyIsoCode",

        -- AccountId: transform KD-M#### → CUST-M#### to match Account.Id
        CASE
            WHEN TRIM(kunden_ref) LIKE 'KD-%'
                THEN REGEXP_REPLACE(TRIM(kunden_ref), '^KD-', 'CUST-')
            ELSE TRIM(kunden_ref)
        END AS "AccountId",

        -- Audit fields (static defaults for initial load)
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"

    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT * FROM opp_raw;