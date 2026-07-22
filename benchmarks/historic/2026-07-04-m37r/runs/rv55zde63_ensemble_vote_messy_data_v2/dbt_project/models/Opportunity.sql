-- models/Opportunity.sql
{{ config(materialized='table') }}

WITH source_opportunity AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
    id AS "Id",
    COALESCE(name, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('in prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identification of decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation', 'review') THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN closedate IS NULL THEN '2999-12-31' -- Fallback for NULL source dates, as target is NOT NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE '2999-12-31' -- Fallback for unparseable dates, as target is NOT NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL THEN NULL
        ELSE
            CASE
                WHEN REGEXP_REPLACE(
                        REPLACE(
                            REPLACE(LOWER(TRIM(amount)), 'eur ', ''),
                            '.', ''
                        ),
                        ',', '.'
                    ) ~ '^[+-]?\d+(\.\d+)?$'
                THEN
                    CAST(
                        REGEXP_REPLACE(
                            REPLACE(
                                REPLACE(LOWER(TRIM(amount)), 'eur ', ''),
                                '.', ''
                            ),
                            ',', '.'
                        ) AS DOUBLE PRECISION
                    )
                ELSE NULL
            END
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_opportunity