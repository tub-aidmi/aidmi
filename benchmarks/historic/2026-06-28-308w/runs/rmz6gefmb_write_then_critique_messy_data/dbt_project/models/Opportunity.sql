
{{ config(materialized='table') }}

SELECT
    src."Id" AS "Id",
    COALESCE(NULLIF(TRIM(src."Name"), ''), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(src."StageName")) IN ('PROSPECT', 'PROSPECTING', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(src."StageName")) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(src."StageName")) IN ('LOST', 'VERLOREN', 'CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(src."StageName")) IN ('GEWONNEN', 'CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(src."StageName")) = 'IN PRÜFUNG' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default to Prospecting as StageName is NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN src."CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' THEN src."CloseDate" -- YYYY-MM-DD
            WHEN src."CloseDate" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src."CloseDate", 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN src."CloseDate" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src."CloseDate", 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            WHEN src."CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src."CloseDate", 'MM/DD/YYYY'), 'YYYY-MM-DD') -- M/D/YYYY
            ELSE NULL -- Prefer NULL over sentinel dates if unparseable.
        END,
        '1900-01-01' -- Fallback for NOT NULL target column
    ) AS "CloseDate",
    CASE
        WHEN TRIM(src."Amount") ~ '^(None|null|NULL)$' THEN NULL
        WHEN src."Amount" IS NULL THEN NULL
        ELSE CAST(
            REPLACE(
                REPLACE(
                    REGEXP_REPLACE(TRIM(src."Amount"), '[^0-9,.-]', '', 'g'),
                    '.', ''
                ),
                ',', '.'
            ) AS DOUBLE PRECISION
        )
    END AS "Amount",
    src."CurrencyIsoCode" AS "CurrencyIsoCode",
    src."AccountId" AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }} src
