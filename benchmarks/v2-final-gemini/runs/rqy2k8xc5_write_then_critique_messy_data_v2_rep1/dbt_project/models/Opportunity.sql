{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(opp.stagename)) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(opp.stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN') THEN 'Closed Lost'
        WHEN UPPER(TRIM(opp.stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(opp.stagename)) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI', 'IN PRÜFUNG') THEN 'Qualification'
        WHEN UPPER(TRIM(opp.stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(opp.stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(opp.stagename)) IN ('ID. DECISION MAKERS', 'IDENTIFY DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(opp.stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(opp.stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL', 'PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(opp.stagename)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION', 'REVIEW') THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN opp.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE '1900-01-01'
    END AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(opp.amount, '[^0-9.,-]+', '', 'g') ~ '^[+-]?\s*\d+(\.\d{3})*,\d+$' THEN
            CAST(REPLACE(REPLACE(REGEXP_REPLACE(opp.amount, '[^0-9.,-]+', '', 'g'), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN REGEXP_REPLACE(opp.amount, '[^0-9.,-]+', '', 'g') ~ '^[+-]?\s*\d+\.?\d*$' THEN
            CAST(REGEXP_REPLACE(opp.amount, '[^0-9.,-]+', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opp.currencyisocode AS "CurrencyIsoCode",
    opp.accountid AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp
