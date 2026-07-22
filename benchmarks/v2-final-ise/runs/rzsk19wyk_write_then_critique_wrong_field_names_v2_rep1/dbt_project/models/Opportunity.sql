{{ config(materialized='table') }}

SELECT 
    ch."chance_id" AS "Id",
    COALESCE(TRIM(ch."bezeichnung"), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(ch."phase")) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(ch."phase")) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(ch."phase")) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(ch."phase")) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(ch."phase")) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(ch."phase")) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(ch."phase")) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(ch."phase")) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(ch."phase")) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(ch."phase")) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE 
        WHEN ch."abschlussdatum" IS NOT NULL AND ch."abschlussdatum" ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE(ch."abschlussdatum", 'YYYY-MM-DD')::TEXT
        WHEN ch."abschlussdatum" IS NOT NULL AND ch."abschlussdatum" ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_DATE(ch."abschlussdatum", 'DD.MM.YYYY')::TEXT
        WHEN ch."abschlussdatum" IS NOT NULL AND ch."abschlussdatum" ~ '^\d{8}$'
        THEN TO_DATE(ch."abschlussdatum", 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    ch."volumen" AS "Amount",
    ch."waehrung" AS "CurrencyIsoCode",
    LOWER(SUBSTR(MD5('acc_' || TRIM(kd."kunden_nr")), 1, 15)) AS "AccountId",
    ch."chance_id" AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} ch
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kd 
    ON TRIM(ch."kd_nr") = TRIM(kd."kunden_nr")