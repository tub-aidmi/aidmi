{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(INITCAP(TRIM(o.name)), 'Unknown') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.stagename)) IN ('PROSPECTING', 'QUALIFICATION', 'NEEDS ANALYSIS', 'VALUE PROPOSITION', 'ID. DECISION MAKERS', 'PERCEPTION ANALYSIS', 'PROPOSAL/PRICE QUOTE', 'NEGOTIATION/REVIEW', 'CLOSED WON', 'CLOSED LOST') 
            THEN INITCAP(TRIM(o.stagename))
        ELSE 'Prospecting'
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.amount ~ '^[\d\s.,-]+$' THEN 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(o.amount, '[^\d.,-]', '', 'g'),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) AS DOUBLE PRECISION
            )
        ELSE NULL
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o