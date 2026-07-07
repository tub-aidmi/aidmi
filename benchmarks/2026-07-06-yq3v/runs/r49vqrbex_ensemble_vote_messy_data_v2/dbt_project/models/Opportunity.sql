-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    src.name AS "Name",
    CASE LOWER(TRIM(src.stagename))
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        (CASE WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(src.closedate, 'YYYY-MM-DD')::TEXT END),
        (CASE WHEN src.closedate ~ '^\d{8}$' THEN TO_DATE(src.closedate, 'YYYYMMDD')::TEXT END),
        (CASE WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(src.closedate, 'DD.MM.YYYY')::TEXT END),
        (CASE WHEN src.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(src.closedate, 'MM/DD/YYYY')::TEXT END),
        '1900-01-01' -- Default for NOT NULL CloseDate if all parsing fails and no format matches
    ) AS "CloseDate",
    CASE
        WHEN src.amount ~ '^[+-]?(\d{1,3}(\.\d{3})*|\d+),\d+$' THEN -- European format (e.g., 1.234,56)
            REPLACE(REPLACE(src.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN src.amount ~ '^[+-]?[[:space:]]*[A-Za-z]{3}[[:space:]]*(\d+(\.\d+)?)$' THEN -- Currency prefix (e.g., EUR 123.45)
            REGEXP_REPLACE(src.amount, '^[+-]?[[:space:]]*[A-Za-z]{3}[[:space:]]*', '')::DOUBLE PRECISION
        WHEN src.amount ~ '^[+-]?\d+(\.\d+)?$' THEN -- Standard decimal (e.g., 123.45)
            src.amount::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    src.currencyisocode AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src