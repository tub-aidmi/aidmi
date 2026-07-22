{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'in prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD') -- YYYY-MM-DD
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY') -- DD.MM.YYYY
            WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD') -- YYYYMMDD
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
            ELSE NULL
        END,
        '1900-01-01' -- Default value for NOT NULL CloseDate if parsing fails
    )::text AS "CloseDate",
    CASE
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')) ~ '^-?\d+\.\d+,\d+$' THEN REPLACE(REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')) ~ '^-?\d+,\d+$' THEN REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g')) ~ '^-?\d+\.?\d*$' THEN TRIM(REGEXP_REPLACE(amount, '[^0-9.-]', '', 'g'))::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source(source_name, source_table) }}
