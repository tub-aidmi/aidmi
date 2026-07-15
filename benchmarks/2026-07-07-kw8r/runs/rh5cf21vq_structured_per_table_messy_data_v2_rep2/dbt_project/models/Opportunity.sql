{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    COALESCE(TRIM(o.name), 'Unnamed Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(TRIM(o.stagename)) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(TRIM(o.stagename)) = 'qualification' THEN 'Qualification'
            WHEN LOWER(TRIM(o.stagename)) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(o.stagename)) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(TRIM(o.stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(o.stagename)) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposal & price quote') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation review', 'negotiations') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(o.stagename)) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(TRIM(o.stagename)) = 'closed lost' THEN 'Closed Lost'
            ELSE NULL
        END,
        'Qualification'
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.closedate, 'DD.MM.YYYY')::TEXT
            WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.closedate, 'YYYY-MM-DD')::TEXT
            WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(o.closedate, 'MM/DD/YYYY')::TEXT
            WHEN o.closedate ~ '^\d{8}$' THEN
                CASE
                    WHEN SUBSTR(o.closedate, 5, 2) BETWEEN '01' AND '12'
                         AND SUBSTR(o.closedate, 7, 2) BETWEEN '01' AND '31'
                    THEN TO_DATE(o.closedate, 'YYYYMMDD')::TEXT
                    ELSE NULL
                END
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CASE 
        WHEN TRIM(o.amount) IS NULL OR TRIM(o.amount) = '' THEN NULL
        WHEN o.amount !~ '\d' THEN NULL
        ELSE
            CASE
                WHEN o.amount ~ '\d+\.\d{3},\d+' THEN 
                    REPLACE(REPLACE(o.amount, '.', ''), ',', '.')::DOUBLE PRECISION
                WHEN o.amount ~ '\d+,\d{3}\.\d+' THEN 
                    REPLACE(o.amount, ',', '')::DOUBLE PRECISION
                ELSE
                    CASE
                        WHEN REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g') = '' THEN NULL
                        ELSE CAST(REGEXP_REPLACE(o.amount, '[^0-9.,]', '', 'g') AS DOUBLE PRECISION)
                    END
            END
    END AS "Amount",
    UPPER(TRIM(o.currencyisocode)) AS "CurrencyIsoCode",
    a.id AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
    ON TRIM(o.accountid) = TRIM(a.id)

WHERE TRIM(o.id) <> ''