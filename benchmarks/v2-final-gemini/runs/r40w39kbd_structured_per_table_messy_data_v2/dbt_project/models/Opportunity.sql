-- {{ config(materialized='table') }}
SELECT
    opp.id AS "Id",
    COALESCE(TRIM(opp.name), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(opp.stagename) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(opp.stagename) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(opp.stagename) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(opp.stagename) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(opp.stagename) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(opp.stagename) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(opp.stagename) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(opp.stagename) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(opp.stagename) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(opp.stagename) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target column
    END AS "StageName",
    CASE
        WHEN TRIM(opp.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(opp.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(opp.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(opp.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opp.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01' -- Default for NOT NULL target column
    END AS "CloseDate",
    CASE
        WHEN TRIM(opp.amount) ~ '^[ ]*\d+([\.,]\d+)?$' THEN
            CAST(REPLACE(REPLACE(TRIM(opp.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(opp.currencyisocode) AS "CurrencyIsoCode",
    opp.accountid AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp