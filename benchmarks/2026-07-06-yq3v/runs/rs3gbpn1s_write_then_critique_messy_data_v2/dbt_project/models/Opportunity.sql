{{ config(materialized='table') }}'''''SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.stagename) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN TRIM(o.stagename)
        ELSE 'Prospecting' -- Default for invalid or NULL stagename
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(TRIM(o.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE('2000-01-01', 'YYYY-MM-DD'), 'YYYY-MM-DD') -- Default date for NULL or unparseable
    ) AS "CloseDate",
    CASE
        WHEN o.amount ~ '^\s*[$]?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*$' THEN -- Basic check for numeric format
            CAST(REPLACE(REPLACE(REPLACE(TRIM(o.amount), '$', ''), ',', ''), '.', '') AS DOUBLE PRECISION) / 100 -- Assuming format like $1,234.56 or 1.234,56 (European) or 1234.56. Try to be robust
        WHEN o.amount ~ '^\s*\d{1,3}\.\d{3},\d{2}\s*$' THEN -- European 1.234,56
            CAST(REPLACE(REPLACE(TRIM(o.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^\s*\d{1,3},\d{3}\.\d{2}\s*$' THEN -- US 1,234.56
            CAST(REPLACE(TRIM(o.amount), ',', '') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^\s*\d+(?:\.\d+)?\s*$' THEN -- Simple decimal like 1234.56
            CAST(TRIM(o.amount) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(o.currencyisocode) AS "CurrencyIsoCode",
    TRIM(o.accountid) AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS o