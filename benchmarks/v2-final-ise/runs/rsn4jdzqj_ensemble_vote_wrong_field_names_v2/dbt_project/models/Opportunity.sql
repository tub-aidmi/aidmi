{{ config(materialized='table') }}

SELECT 
    "chance_id" AS "Id",
    COALESCE(NULLIF(TRIM("bezeichnung"), ''), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM("phase")) IN ('PROSPEKTING', 'PROSPEKT') THEN 'Prospecting'
        WHEN UPPER(TRIM("phase")) IN ('QUALIFIZIERUNG', 'QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM("phase")) IN ('BEDARFSANALYSE', 'NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM("phase")) IN ('WERTVORSCHLAG', 'VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM("phase")) IN ('ENTSCHEIDUNGSTRÄGER', 'ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM("phase")) IN ('WAHRNEHMUNGSANALYSE', 'PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM("phase")) IN ('ANGEBOT', 'PROPOSAL', 'PREISANGEBOT') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM("phase")) IN ('VERHANDLUNG', 'NEGOTIATION', 'REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM("phase")) IN ('GESCHLOSSEN GEWONNEN', 'CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM("phase")) IN ('GESCHLOSSEN VERLOREN', 'CLOSED LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN "abschlussdatum" ~ '^\d{4}-\d{2}-\d{2}$' THEN "abschlussdatum"
        WHEN "abschlussdatum" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("abschlussdatum", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "abschlussdatum" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE("abschlussdatum", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "abschlussdatum" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("abschlussdatum", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    "volumen" AS "Amount",
    CASE 
        WHEN UPPER(TRIM("waehrung")) = 'EUR' THEN 'EUR'
        WHEN UPPER(TRIM("waehrung")) = 'USD' THEN 'USD'
        WHEN UPPER(TRIM("waehrung")) = 'GBP' THEN 'GBP'
        WHEN UPPER(TRIM("waehrung")) = 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    "kunden"."kunden_nr" AS "AccountId",
    "chance_id" AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS "chancen"
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS "kunden"
    ON "chancen"."kd_nr" = "kunden"."kunden_nr"