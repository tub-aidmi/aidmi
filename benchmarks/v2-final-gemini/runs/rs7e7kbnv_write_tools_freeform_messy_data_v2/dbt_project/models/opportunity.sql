{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(stagename) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(stagename) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(stagename) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(stagename) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(stagename) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(stagename) IN ('proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(stagename) IN ('negotiation/review', 'in prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(stagename) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate -- YYYY-MM-DD
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
        ELSE '2000-01-01' -- Fallback for unparseable dates or NULL
    END AS "CloseDate",
    CAST(
        CASE
            WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
            -- Handle European format (e.g., 1.234,56): remove thousands dots, replace comma with dot
            WHEN amount ~ '^\S*\d{1,3}(\.\d{3})*,\d{2}$' THEN
                REPLACE(REPLACE(REGEXP_REPLACE(amount, '[^0-9,\.]', '', 'g'), '.', ''), ',', '.')
            -- Handle standard format (e.g., 1,234.56): remove thousands commas
            WHEN amount ~ '^\S*\d{1,3}(,\d{3})*\.\d{2}$' THEN
                REPLACE(REGEXP_REPLACE(amount, '[^0-9,\.]', '', 'g'), ',', '')
            -- Handle integers or other formats that might contain currency symbols
            WHEN amount ~ '^-?\d+(\.\d+)?$' OR amount ~ '^-?\d+(,\d+)?$' THEN
                REGEXP_REPLACE(REPLACE(amount, ',', '.'), '[^0-9.]', '', 'g')
            ELSE NULL
        END AS DOUBLE PRECISION
    ) AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}