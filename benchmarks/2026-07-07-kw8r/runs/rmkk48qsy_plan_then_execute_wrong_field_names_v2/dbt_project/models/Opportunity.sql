{{ config(materialized='table') }}

SELECT
    '006' || TRIM(chance_id) AS "Id",
    TRIM(INITCAP(bezeichnung)) AS "Name",
    CASE UPPER(TRIM(phase))
        WHEN 'AKQUISE' THEN 'Prospecting'
        WHEN 'PROSPEKTION' THEN 'Prospecting'
        WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ENTSCHEIDUNGSTREFFER' THEN 'Id. Decision Makers'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PRICING' THEN 'Proposal/Price Quote'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG' THEN 'Negotiation/Review'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'ABSCHLUSS ERFOLGREICH' THEN 'Closed Won'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'ABSCHLUSS FEHLGESCHLAGEN' THEN 'Closed Lost'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN TRIM(abschlussdatum) IS NULL OR TRIM(abschlussdatum) = '' THEN CAST(CURRENT_DATE AS TEXT)
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(abschlussdatum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(abschlussdatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE CAST(TO_DATE(TRIM(abschlussdatum), 'YYYY-MM-DD') AS TEXT)
    END AS "CloseDate",
    CASE 
        WHEN volumen IS NULL THEN NULL
        ELSE CAST(volumen AS DOUBLE PRECISION)
    END AS "Amount",
    NULLIF(UPPER(TRIM(waehrung)), '') AS "CurrencyIsoCode",
    '001' || TRIM(kd_nr) AS "AccountId",
    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}