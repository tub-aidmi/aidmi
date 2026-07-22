{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'WON') THEN 'Closed Won'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN', 'LOST') THEN 'Closed Lost'
        WHEN UPPER(NULLIF(TRIM(stagename), '')) IN ('IN KONTAKT', 'IN PRÜFUNG') THEN 'Prospecting'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN NULLIF(TRIM(closedate), '') IS NULL THEN NULL
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN NULLIF(TRIM(amount), '') IS NULL THEN NULL
        WHEN amount ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(amount, ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+\.[0-9]{2}$' THEN CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+$' THEN CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^-?[0-9]+\.[0-9]{2}$' THEN CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^-?[0-9]+$' THEN CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^EUR [0-9]+\.[0-9]{2}$' THEN CAST(REGEXP_REPLACE(amount, '^EUR ', '') AS DOUBLE PRECISION)
        WHEN amount ~ '^EUR [0-9]+$' THEN CAST(REGEXP_REPLACE(amount, '^EUR ', '') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(currencyisocode), '') AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
