{{ config(materialized='table') }}

SELECT 
    UPPER(TRIM(chance_id)) AS "Id",
    COALESCE(UPPER(TRIM(bezeichnung)), 'Unknown') AS "Name",
    CASE 
        WHEN UPPER(TRIM(phase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(phase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(phase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(phase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(phase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(phase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(phase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(phase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(phase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(phase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN TRIM(abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(abschlussdatum), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    CASE 
        WHEN UPPER(TRIM(waehrung)) IN ('EUR', 'USD', 'GBP', 'CHF') THEN UPPER(TRIM(waehrung))
        ELSE NULL
    END AS "CurrencyIsoCode",
    UPPER(TRIM(kd_nr)) AS "AccountId",
    UPPER(TRIM(chance_id)) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}