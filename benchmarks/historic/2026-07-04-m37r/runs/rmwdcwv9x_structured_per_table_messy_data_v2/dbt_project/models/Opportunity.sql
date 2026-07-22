-- depends_on: {{ ref('Account') }} This is a placeholder comment to satisfy the thought process - will be removed.

{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(o.stagename))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'in prüfung' THEN 'Qualification'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for unmapped or NULL values
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1970-01-01' -- Default for unparseable or NULL dates, as target is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN TRIM(o.amount) ~ '^[+-]?\d{1,3}(\.\d{3})*,\d+$' THEN
            CAST(REPLACE(REPLACE(TRIM(o.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(o.amount) ~ '^[+-]?\d+(\.\d+)?$' THEN
            CAST(TRIM(o.amount) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o