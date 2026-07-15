{{ config(materialized='table') }}

SELECT
    -- Id: use chance_id directly, also populate Legacy_Opportunity_ID__c
    c.chance_id AS "Id",

    -- Name from bezeichnung; fallback when empty
    TRIM(
        CASE 
            WHEN TRIM(COALESCE(c.bezeichnung, '')) = '' THEN 'Opportunity - ' || c.chance_id
            ELSE TRIM(c.bezeichnung)
        END
    ) AS "Name",

    -- StageName: map German/English phase values to Salesforce Opportunity stages
    INITCAP(
        CASE 
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'vorverkauf' THEN 'Prospecting'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'qualification' THEN 'Qualification'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'qualifikation' THEN 'Qualification'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'bedarfsanalyse' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('wertpropotion', 'wertangebot') THEN 'Value Proposition'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'entscheideridentifikation' THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'wahrnehmungsanalyse' THEN 'Perception Analysis'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'verhandlung' THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('abgeschlossen gewonnen', 'gewonnen') THEN 'Closed Won'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) = 'closed lost' THEN 'Closed Lost'
            WHEN LOWER(TRIM(COALESCE(c.phase, ''))) IN ('abgeschlossen verloren', 'verloren') THEN 'Closed Lost'
            ELSE NULL
        END
    ) AS "StageName",

    -- CloseDate: parse DD.MM.YYYY or YYYY-MM-DD format to ISO text; else NULL
    CASE 
        WHEN c.abschlussdatum IS NULL OR TRIM(c.abschlussdatum) = '' THEN NULL
        WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(c.abschlussdatum)
        ELSE NULL
    END AS "CloseDate",

    -- Amount: volumen is already double precision; filter zero values out
    CASE 
        WHEN c.volumen IS NOT NULL AND c.volumen != 0 THEN c.volumen::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: map German/English country codes to ISO 4217 currency codes
    INITCAP(
        CASE 
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) = 'DEU' THEN 'EUR'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) = 'EURO' THEN 'EUR'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) IN ('USA', 'USD') THEN 'USD'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) IN ('GBR', 'GBP') THEN 'GBP'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) = 'CHF' THEN 'CHF'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) = 'CAD' THEN 'CAD'
            WHEN UPPER(TRIM(COALESCE(c.waehrung, ''))) = 'JPY' THEN 'JPY'
            ELSE NULL
        END
    ) AS "CurrencyIsoCode",

    -- AccountId: use the ERP number from the joined Kunden record (Salesforce-style Account Id)
    k.erp_nummer AS "AccountId",

    -- Legacy_Opportunity_ID__c: same as chance_id for row-level traceability
    c.chance_id AS "Legacy_Opportunity_ID__c",

    -- CreatedDate, LastModifiedDate, IsDeleted: not available in source; defaults
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(k.kunden_nr) = TRIM(c.kd_nr)