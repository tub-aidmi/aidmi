{{ config(materialized='table') }}

SELECT 
    opp.id AS "Id",
    COALESCE(TRIM(opp.name), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(opp.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(opp.stagename)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opp.stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(opp.stagename)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opp.stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opp.stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opp.stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opp.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opp.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.stagename)) IN ('in kontakt', 'in prüfung') THEN NULL
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN opp.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN opp.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(REPLACE(opp.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[0-9]+,[0-9]+$' THEN 
            CAST(REPLACE(opp.amount, ',', '.') AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST(opp.amount AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^-?[0-9]+\.[0-9]+$' THEN 
            CAST(opp.amount AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^[A-Za-z]+ [0-9]+\.[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(opp.amount, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN opp.amount ~ '^-?[0-9]+$' THEN 
            CAST(opp.amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opp.currencyisocode)) AS "CurrencyIsoCode",
    opp.accountid AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opp