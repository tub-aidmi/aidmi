{{ config(materialized='table') }}
SELECT
    o.id AS "Id",
    COALESCE(INITCAP(TRIM(o.name)), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stagename)) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stagename)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'IN PRÜFUNG', 'IN KONTAKT') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' OR UPPER(TRIM(o.amount)) = 'NONE' THEN NULL
        ELSE
            CASE
                WHEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(o.amount, '[^0-9.,-]', '', 'g'),
                        '\.', '', 'g'
                    ),
                    ',', '.', 'g'
                ) ~ '^-' THEN NULL
                ELSE CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(o.amount, '[^0-9.,-]', '', 'g'),
                            '\.', '', 'g'
                        ),
                        ',', '.', 'g'
                    ) AS DOUBLE PRECISION
                )
            END
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('£', 'GBP') THEN 'GBP'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('€', 'EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('$', 'USD', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(o.currencyisocode)) = 'CHF' THEN 'CHF'
        ELSE UPPER(TRIM(o.currencyisocode))
    END AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON o.accountid = a.id