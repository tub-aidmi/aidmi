{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) LIKE '%decision makers%' OR LOWER(TRIM(stagename)) IN ('id. decision makers', 'id.decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) LIKE '%perception%' OR LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) LIKE '%proposal%' OR LOWER(TRIM(stagename)) LIKE '%quote%' OR LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal / price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) LIKE '%negotiation%' OR LOWER(TRIM(stagename)) LIKE '%review%' OR LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation / review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' OR closedate::TEXT = '0000-00-00' THEN NULL
        WHEN closedate::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(closedate AS DATE)::TEXT
        WHEN closedate::TEXT ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        WHEN closedate::TEXT ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        WHEN closedate::TEXT ~ '^\d{8}$' THEN
            CAST(
                SUBSTR(closedate, 1, 4) || '-' ||
                SUBSTR(closedate, 5, 2) || '-' ||
                SUBSTR(closedate, 7, 2)
            AS DATE)::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(amount) IS NOT NULL AND TRIM(amount) <> '' THEN
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(amount), '^\s*[a-zA-Z$\u00A3\u20ac]*\s*', '', 'g'),
                            '[^0-9.,]', '', 'g'
                         ),
                    '\\.([0-9]{3})([.,])', '\\1\\2', 'g'
                 ),
                ',', '.'
            )::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
