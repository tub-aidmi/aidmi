-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Opportunity') AS "Name",
    -- StageName is NOT NULL, provide a default if unmapped
    COALESCE(CASE
        WHEN LOWER(stagename) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('lost', 'closed lost', 'abgeschlossen (verloren)', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(stagename) IN ('qualifikation', 'qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(stagename) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE NULL
    END, 'Prospecting') AS "StageName",
    -- CloseDate is NOT NULL, provide a default if unparseable
    COALESCE(CASE
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END, TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    CASE
        WHEN amount IS NULL THEN NULL
        ELSE CAST(REGEXP_REPLACE(
            REGEXP_REPLACE(
                TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')),
                '\.', '', 'g' -- remove thousand separators (dots)
            ),
            ',', '.' -- replace comma with dot for decimal
        ) AS DOUBLE PRECISION)
    END AS "Amount",
    TRIM(UPPER(currencyisocode)) AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
