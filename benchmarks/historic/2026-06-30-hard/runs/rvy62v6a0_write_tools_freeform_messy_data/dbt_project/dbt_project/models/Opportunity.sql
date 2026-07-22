{% set source_name %}{{ 'fixture_messy_data_src' }}{% endset %}
{% set table_name %}{{ 'Opportunity' }}{% endset %}

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown') AS "Name", -- Target is NOT NULL
    COALESCE(
        CASE
            WHEN TRIM(LOWER(COALESCE("StageName", ''))) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
            WHEN TRIM(LOWER(COALESCE("StageName", ''))) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
            WHEN TRIM(LOWER(COALESCE("StageName", ''))) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
            WHEN TRIM(LOWER(COALESCE("StageName", ''))) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN TRIM(LOWER(COALESCE("StageName", ''))) = 'in prüfung' THEN 'Negotiation/Review'
            ELSE NULL
        END,
        'Prospecting' -- Default for NOT NULL StageName
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN NULLIF(TRIM(COALESCE("CloseDate", '')), '') ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(COALESCE("CloseDate", '')), ''), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(COALESCE("CloseDate", '')), '') ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(COALESCE("CloseDate", '')), ''), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(COALESCE("CloseDate", '')), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(COALESCE("CloseDate", '')), ''), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN NULLIF(TRIM(COALESCE("CloseDate", '')), '') ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(NULLIF(TRIM(COALESCE("CloseDate", '')), ''), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default for unparseable or NULL CloseDate, as target is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN "Amount" IS NULL THEN NULL
        ELSE CAST(REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    "Amount",
                    '[^0-9,\.\-]', -- Remove any character that is not a digit, comma, dot, or minus sign
                    '',
                    'g'
                ),
                '\.(?=\d{3})', -- Remove dots used as thousand separators (followed by exactly 3 digits)
                '',
                'g'
            ),
            ',', -- Replace comma decimal separator with dot
            '.',
            'g'
        ) AS DOUBLE PRECISION)
    END AS "Amount",
    "CurrencyIsoCode" AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source(source_name, table_name) }}
