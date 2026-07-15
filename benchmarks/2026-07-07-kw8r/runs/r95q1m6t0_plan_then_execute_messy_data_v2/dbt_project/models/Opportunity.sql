{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
account_mapping AS (
    SELECT
        id AS account_id,
        id AS account_sf_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
)

SELECT
    opp.id AS "Id",
    INITCAP(TRIM(COALESCE(opp.name, 'Unknown'))) AS "Name",
    CASE
        WHEN TRIM(LOWER(opp.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost')
            THEN INITCAP(TRIM(opp.stagename))
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN opp.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN opp.amount ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(opp.amount, '\.', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[0-9]+,[0-9]{2}$' THEN 
            CAST(REGEXP_REPLACE(opp.amount, ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[0-9]+\.[0-9]{2}$' THEN 
            CAST(opp.amount AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[0-9]+$' THEN 
            CAST(opp.amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opp.currencyisocode)) AS "CurrencyIsoCode",
    acc.account_sf_id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM opportunity_source opp
LEFT JOIN account_mapping acc ON opp.accountid = acc.account_id