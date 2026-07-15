{{ config(materialized='table') }}

SELECT
    '006' || LPAD(REGEXP_REPLACE(CAST("chance_id" AS VARCHAR), '\D', '', 'g'), 8, '0') AS "Id",
    COALESCE(INITCAP(TRIM("bezeichnung")), 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM("phase"))
        WHEN 'erste kontakte' THEN 'Prospecting'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'bedarfssanalyse' THEN 'Needs Analysis'
        WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
        WHEN 'wertproposition' THEN 'Value Proposition'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'identifizierung entscheidungstrger' THEN 'Id. Decision Makers'
        WHEN 'identifizierung entscheidungsträger' THEN 'Id. Decision Makers'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'angebot/preiskalkulation' THEN 'Proposal/Price Quote'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'verhandlung/prfung' THEN 'Negotiation/Review'
        WHEN 'verhandlung/prüfung' THEN 'Negotiation/Review'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE 'Qualification'
    END AS "StageName",
    CASE
        WHEN TRIM("abschlussdatum") ~ '^\d{8}$'
            AND TO_DATE(TRIM("abschlussdatum"), 'YYYYMMDD') IS NOT NULL
            THEN TO_CHAR(TO_DATE(TRIM("abschlussdatum"), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM("abschlussdatum") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            AND TO_DATE(TRIM("abschlussdatum"), 'FMDD.FMMM.FMYYYY') IS NOT NULL
            THEN TO_CHAR(TO_DATE(TRIM("abschlussdatum"), 'FMDD.FMMM.FMYYYY'), 'YYYY-MM-DD')
        ELSE COALESCE(CURRENT_DATE::TEXT, '1970-01-01')
    END AS "CloseDate",
    "volumen" AS "Amount",
    CASE UPPER(TRIM("waehrung"))
        WHEN 'EUR' THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    '001' || TRIM("kd_nr") AS "AccountId",
    "chance_id" AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}