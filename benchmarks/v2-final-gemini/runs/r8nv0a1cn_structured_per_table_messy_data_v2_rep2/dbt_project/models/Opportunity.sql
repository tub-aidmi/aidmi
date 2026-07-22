{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.stagename) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(opp.stagename) = 'qualification' THEN 'Qualification'
        WHEN LOWER(opp.stagename) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(opp.stagename) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(opp.stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.stagename) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(opp.stagename) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.stagename) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(opp.stagename) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(opp.stagename) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN opp.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.closedate::DATE -- YYYY-MM-DD
                WHEN opp.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(opp.closedate, 'DD.MM.YYYY') -- DD.MM.YYYY
                WHEN opp.closedate ~ '^\d{8}$' THEN TO_DATE(opp.closedate, 'YYYYMMDD') -- YYYYMMDD
                WHEN opp.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(opp.closedate, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL `CloseDate` if source is unparseable or NULL
    ) AS "CloseDate",
    CASE
        WHEN opp.amount IS NULL OR TRIM(opp.amount) = '' THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g') LIKE '%.%,%'
                         AND POSITION('.' IN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g')) < POSITION(',' IN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g')) THEN -- European format like 1.234,56
                        REPLACE(REPLACE(REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g'), '.', ''), ',', '.')
                    WHEN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g') LIKE '%,'.%'
                         AND POSITION(',' IN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g')) < POSITION('.' IN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g')) THEN -- US format like 1,234.56
                        REPLACE(REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g'), ',', '')
                    WHEN REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g') LIKE '%,%' THEN -- Only comma, assume European decimal like 123,45
                        REPLACE(REGEXP_REPLACE(opp.amount, '[^0-9.,-]', '', 'g'), ',', '.')
                    ELSE -- Only dot, or no separators like 123.45 or 12345
                        REGEXP_REPLACE(opp.amount, '[^0-9.-]', '', 'g')
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    opp.currencyisocode AS "CurrencyIsoCode",
    opp.accountid AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not in source
    NULL AS "LastModifiedDate", -- Not in source
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp;