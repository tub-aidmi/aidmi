{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) IN ('qualification', 'qualifikation', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) = 'in kontakt' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(
                TO_DATE(o.closedate, 'MM/DD/YYYY'),
                'YYYY-MM-DD'
            )
        WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_CHAR(
                TO_DATE(o.closedate, 'DD.MM.YYYY'),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(o.amount) = '' OR LOWER(TRIM(o.amount)) = 'none' THEN NULL
        -- European format with dot-thousands and comma-decimal: e.g. "60.702,05" or "150.721,39"
        WHEN o.amount ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
            REGEXP_REPLACE(o.amount, '[^0-9\-.,]', '')  -- remove any currency symbols
            ::TEXT
            , CASE
                WHEN REGEXP_SUBSTR(o.amount, ',\d+$') IS NOT NULL
                THEN (REGEXP_REPLACE(o.amount, '[^\d.\-,]', ''))::DOUBLE PRECISION
                ELSE NULL
              END
        ELSE CAST(REGEXP_REPLACE(o.amount, '[^\d.\-]', '') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE UPPER(TRIM(o.currencyisocode))
        WHEN 'USD' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN '$' THEN 'USD'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN '£' THEN 'GBP'
        WHEN '€' THEN 'EUR'
        ELSE UPPER(TRIM(o.currencyisocode))
    END AS "CurrencyIsoCode",
    CAST(o.accountid AS TEXT) AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o