{{ config(materialized='table') }}

SELECT
    -- Id: Transform chance_id to Salesforce ID format (prefix '006' + LPAD to 18 chars)
    '006' || LPAD(REGEXP_REPLACE(TRIM(c.chance_id), '[^0-9]', ''), 15, '0') AS "Id",

    -- Name: INITCAP(TRIM(bezeichnung)) with fallback for NULLs
    COALESCE(INITCAP(TRIM(c.bezeichnung)), 'Unnamed Opportunity') AS "Name",

    -- StageName: Map source phase (DE/EN) to SFDC stage enum — fixed syntax: each WHEN must be a single value
    CASE UPPER(TRIM(c.phase))
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'VALUE PROP' THEN 'Value Proposition'
        WHEN 'IDENTIFY DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'PROPOSAL / PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'NEGOTIATION / REVIEW' THEN 'Negotiation/Review'
        ELSE 'Prospecting'
    END AS "StageName",

    -- CloseDate: Parse multiple text date formats to ISO YYYY-MM-DD string; NULL if unparseable
    CASE
        WHEN TRIM(c.abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(c.abschlussdatum)
        WHEN TRIM(c.abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(c.abschlussdatum) ~ '^\d{8}$' THEN
            SUBSTR(TRIM(c.abschlussdatum), 1, 4) || '-' ||
            SUBSTR(TRIM(c.abschlussdatum), 5, 2) || '-' ||
            SUBSTR(TRIM(c.abschlussdatum), 7, 2)
        WHEN TRIM(c.abschlussdatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: Cast double precision with fallback to 0.0 for missing values
    COALESCE(c.volumen, 0.0) AS "Amount",

    -- CurrencyIsoCode: Map source waehrung to ISO 4217 code; fixed syntax (multiple WHEN lines)
    CASE UPPER(TRIM(c.waehrung))
        WHEN 'EUR' THEN 'EUR'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'USD' THEN 'USD'
        WHEN 'US-DOLLAR' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'GBP' THEN 'GBP'
        WHEN 'POUND STERLING' THEN 'GBP'
        WHEN 'POUND' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        WHEN 'SWISS FRANC' THEN 'CHF'
        ELSE 'EUR'
    END AS "CurrencyIsoCode",

    -- AccountId: Transform kunden.kunden_nr to Salesforce Account ID format (prefix '001')
    '001' || LPAD(REGEXP_REPLACE(TRIM(k.kunden_nr), '[^0-9]', ''), 15, '0') AS "AccountId",

    -- Legacy_Opportunity_ID__c: Direct copy of source natural key
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",

    -- Audit fields (not in source — deterministic defaults)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(c.kd_nr) = TRIM(k.kunden_nr)