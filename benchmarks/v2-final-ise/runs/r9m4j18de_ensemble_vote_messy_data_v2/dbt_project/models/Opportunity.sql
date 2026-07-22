{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(o.stagename))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.closedate IS NULL OR TRIM(o.closedate) = '' THEN NULL
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate  -- YYYY-MM-DD already ISO
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN 
            SUBSTR(o.closedate, 1, 4) || '-' || SUBSTR(o.closedate, 5, 2) || '-' || SUBSTR(o.closedate, 7, 2)
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(o.amount, '[^\d.,]', '', 'g'),
                '(\d)\.(\d{3})([,|]|$)', '\1\2', 'g'  -- Remove European thousand-separator dots
            ),
            ',', '.'  -- Swap comma to decimal point
        )::DOUBLE PRECISION
    END AS "Amount",
    UPPER(TRIM(o.currencyisocode)) AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o