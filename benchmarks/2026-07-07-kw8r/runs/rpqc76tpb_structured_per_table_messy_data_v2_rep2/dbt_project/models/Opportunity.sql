{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposal & price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation review', 'negotiations') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        WHEN closedate ~ '^\d{8}$' THEN
            CASE
                WHEN SUBSTR(closedate, 5, 2) BETWEEN '01' AND '12'
                     AND SUBSTR(closedate, 7, 2) BETWEEN '01' AND '31'
                THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,]', '', 'g'), '^\.+', '')::DOUBLE PRECISION
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}

WHERE TRIM(id) <> ''