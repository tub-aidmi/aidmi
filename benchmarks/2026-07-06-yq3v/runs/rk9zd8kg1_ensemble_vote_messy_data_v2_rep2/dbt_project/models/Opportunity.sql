{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
cleaned_amount_step1 AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid,
        CASE
            WHEN source_data.amount IS NULL OR TRIM(source_data.amount) = '' THEN NULL
            ELSE
                TRIM(REGEXP_REPLACE(source_data.amount, '[^0-9.,-]+', '', 'g'))
        END AS cleaned_value
    FROM source_data
)
SELECT
    cleaned_amount_step1.id AS "Id",
    COALESCE(cleaned_amount_step1.name, 'Unknown Opportunity') AS "Name",
    CASE UPPER(TRIM(cleaned_amount_step1.stagename))
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'WON' THEN 'Closed Won'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        WHEN 'LOST' THEN 'Closed Lost
        WHEN 'VERLOREN' THEN 'Closed Lost'
        WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'QUALIFIKATION' THEN 'Qualification'
        WHEN 'QUALI' THEN 'Qualification'
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'PROSPECT' THEN 'Prospecting'
        WHEN 'IN KONTAKT' THEN 'Prospecting'
        WHEN 'IN PRÜFUNG' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for unmapped or NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN cleaned_amount_step1.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(cleaned_amount_step1.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN cleaned_amount_step1.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(cleaned_amount_step1.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN cleaned_amount_step1.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(cleaned_amount_step1.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN cleaned_amount_step1.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(cleaned_amount_step1.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for unparseable or NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Fallback if the above CASE results in NULL after parsing
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN cleaned_amount_step1.cleaned_value IS NULL OR TRIM(cleaned_amount_step1.cleaned_value) = '' THEN NULL
            WHEN cleaned_amount_step1.cleaned_value LIKE '%.%' AND cleaned_amount_step1.cleaned_value LIKE '%,%' THEN -- European with both dot (thousands) and comma (decimal)
                REPLACE(REPLACE(cleaned_amount_step1.cleaned_value, '.', ''), ',', '.')
            WHEN cleaned_amount_step1.cleaned_value LIKE '%,%' THEN -- European with only comma (decimal)
                REPLACE(cleaned_amount_step1.cleaned_value, ',', '.')
            ELSE -- Standard (dot is decimal or no decimal)
                cleaned_amount_step1.cleaned_value
        END
    AS DOUBLE PRECISION) AS "Amount",
    cleaned_amount_step1.currencyisocode AS "CurrencyIsoCode",
    cleaned_amount_step1.accountid AS "AccountId",
    cleaned_amount_step1.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cleaned_amount_step1