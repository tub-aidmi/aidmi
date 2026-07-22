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
        WHEN TRIM(abgeschlossendatum) IS NULL OR TRIM(abgeschlossendatum) = '' THEN CURRENT_DATE
        WHEN TRIM(abgeschlossendatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abgeschlossendatum), 'DD.MM.YYYY')
        WHEN TRIM(abgeschlossendatum) ~ '^\d{8}$' THEN TO_DATE(TRIM(abgeschlossendatum), 'YYYYMMDD')
        ELSE TO_DATE(TRIM(abgeschlossendatum), 'YYYY-MM-DD')
    END AS "CloseDate",
    CASE 
        WHEN TRIM(volumen) IS NULL OR TRIM(volumen) = '' THEN NULL
        WHEN TRIM(volumen) ~ '^\d{1,3}\.\d{3},\d+$' THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(volumen), '\.', '', 'g'), ',', '.', 'g')::DOUBLE PRECISION
        ELSE REGEXP_REPLACE(TRIM(volumen), '[^0-9.,]', '', 'g')::DOUBLE PRECISION
    END AS "Amount",
    NULLIF(UPPER(TRIM(waehrung)), '') AS "CurrencyIsoCode",
    '001' || TRIM(kd_nr) AS "AccountId",
    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}