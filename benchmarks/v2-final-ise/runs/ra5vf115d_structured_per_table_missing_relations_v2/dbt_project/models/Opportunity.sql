{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE LOWER(TRIM(o.stage))
        WHEN 'new' THEN 'Prospecting'
        WHEN 'lead' THEN 'Prospecting'
        WHEN 'qualified' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'proposal' THEN 'Proposal/Price Quote'
        WHEN 'quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'closed' THEN 'Closed Won'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.close_date IS NOT NULL AND TRIM(o.close_date) != '' THEN
            -- Handle various date formats: YYYY-MM-DD, DD.MM.YYYY, MM/DD/YYYY, YYYYMMDD
            CAST(
                CASE
                    WHEN o.close_date ~ '^\d{4}-\d{2}-\d{2}$' THEN o.close_date::DATE
                    WHEN o.close_date ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.close_date, 'DD.MM.YYYY')::DATE
                    WHEN o.close_date ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(o.close_date, 'YYYY/MM/DD')::DATE
                    WHEN o.close_date ~ '^\d{8}$' THEN TO_DATE(o.close_date, 'YYYYMMDD')::DATE
                    ELSE NULL
                END
            AS TEXT)
        ELSE '1900-01-01'
    END AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON TRIM(o.customer_number) = TRIM(acc.id)