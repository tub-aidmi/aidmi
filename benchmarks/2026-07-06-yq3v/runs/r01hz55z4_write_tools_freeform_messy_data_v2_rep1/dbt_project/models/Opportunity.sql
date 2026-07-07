{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    COALESCE(CASE
        WHEN LOWER(stagename) IN ('prospecting', 'in kontakt', 'prospect') THEN 'Prospecting'
        WHEN LOWER(stagename) IN ('qualification', 'qualifikation', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('won', 'gewonnen', 'closed won', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Fallback for NOT NULL target
    END, 'Prospecting') AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '{{ default_date }}'
    ) AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CAST(
                NULLIF(
                    CASE
                        -- All POSITION calls now correctly use 'IN' keyword
                        WHEN POSITION(',' IN REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g')) > 0
                             AND POSITION('.' IN REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g')) > 0
                             AND POSITION('.' IN REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g')) < POSITION(',' IN REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g'))
                        THEN REPLACE(REPLACE(REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.')
                        ELSE REPLACE(REGEXP_REPLACE(amount, '[^0-9,.-]', '', 'g'), ',', '.')
                    END,
                    ''
                ) AS DOUBLE PRECISION
            )
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CAST('{{ default_date }}' AS TEXT) AS "CreatedDate",
    CAST('{{ default_date }}' AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}