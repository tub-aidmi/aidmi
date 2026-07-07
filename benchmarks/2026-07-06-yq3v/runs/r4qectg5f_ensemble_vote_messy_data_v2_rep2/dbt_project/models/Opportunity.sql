{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, 'N/A') AS "Name",
    CASE
        WHEN LOWER(src.stagename) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)', 'closedwon') THEN 'Closed Won'
        WHEN LOWER(src.stagename) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)', 'closedlost') THEN 'Closed Lost'
        WHEN LOWER(src.stagename) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(src.stagename) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(src.stagename) IN ('in prufung') THEN 'Negotiation/Review' -- Assuming 'In Prüfung' (in review) maps to Negotiation/Review
        ELSE 'Prospecting' -- Default to Prospecting for unknown stages as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN src.closedate -- YYYY-MM-DD
            WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN src.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            WHEN src.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY or MM/DD/YYYY
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default to current date if parsing fails
    ) AS "CloseDate",
    CASE
        WHEN src.amount IS NULL OR TRIM(src.amount) = '' THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,-]', '', 'g') ~ '.*\.\d+,\d+$' -- European with dot thousand separator and comma decimal (e.g., 1.234,56)
                        THEN REPLACE(REPLACE(REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,-]', '', 'g'), '.', '', 'g'), ',', '.')
                    WHEN REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,-]', '', 'g') ~ '^-?\d+,\d+$' -- European with comma decimal only (e.g., 123,45)
                        THEN REPLACE(REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,-]', '', 'g'), ',', '.')
                    ELSE -- US format, or no special separators (e.g., 1,234.56, 123.45, 12345)
                        REPLACE(REGEXP_REPLACE(TRIM(src.amount), '[^0-9.,-]', '', 'g'), ',', '')
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    src.currencyisocode AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src
