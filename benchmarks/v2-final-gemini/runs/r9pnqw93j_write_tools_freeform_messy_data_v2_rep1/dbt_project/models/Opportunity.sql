-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(stagename) IN ('prospecting', 'in kontakt', 'prospect') THEN 'Prospecting'
        WHEN LOWER(stagename) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('needs analysis', 'in prüfung') THEN 'Needs Analysis'
        WHEN LOWER(stagename) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(stagename) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(stagename) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(stagename) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(stagename) IN ('negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(stagename) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default value for StageName as it is NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate::DATE
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default date if unparseable/NULL as it is NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CASE
                -- Clean the string by removing non-numeric characters (except . , -), then removing dots for European format, then replacing comma with dot
                -- Then check if the cleaned string is empty or does not match a numeric pattern
                WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g'), '\.', '', 'g'), ',', '.', 'g')) = '' THEN NULL
                WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g'), '\.', '', 'g'), ',', '.', 'g')) !~ '^-?[0-9]+(\.[0-9]+)?$' THEN NULL
                ELSE CAST(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g'), '\.', '', 'g'), ',', '.', 'g')) AS DOUBLE PRECISION)
            END
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
