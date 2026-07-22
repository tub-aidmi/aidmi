{{ config(materialized='table') }}

SELECT 
    -- Id: Transform chance_id to Salesforce-style with OPP prefix (strip existing prefix if present)
    'OPP-' || REGEXP_REPLACE(TRIM(chance_id), '^OPP-*', '', 'i') AS "Id",

    -- Name: TRIM(bezeichnung), default to 'Unnamed Opportunity' if empty/NULL
    CASE 
        WHEN TRIM(bezeichnung) IS NULL OR TRIM(bezeichnung) = '' THEN 'Unnamed Opportunity'
        ELSE TRIM(bezeichnung)
    END AS "Name",

    -- StageName: Map source pipeline stages (English in this dataset) to target enum
    CASE UPPER(TRIM(phase))
        WHEN 'NEUKUNDENGEWINNUNG' THEN 'Prospecting'
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN 'ENTSCHEIDER IDENTIFIZIEREN' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'GEWONNEN' THEN 'Closed Won'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'VERLOREN' THEN 'Closed Lost'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: Parse multiple date formats, return ISO YYYY-MM-DD or NULL
    CASE 
        WHEN TRIM(abschlussdatum) IS NULL OR TRIM(abschlussdatum) = '' THEN NULL
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(abschlussdatum)
        WHEN TRIM(abschlussdatum) ~ '^\d{8}$' THEN TO_DATE(TRIM(abschlussdatum), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: source volumen is already DOUBLE PRECISION — direct cast only
    CAST(volumen AS DOUBLE PRECISION) AS "Amount",

    -- CurrencyIsoCode: normalize currency names to ISO codes
    CASE UPPER(TRIM(waehrung))
        WHEN 'EURO' THEN 'EUR'
        WHEN 'USD-DOLLAR' THEN 'USD'
        WHEN 'GBP-POUND' THEN 'GBP'
        WHEN 'CHF-SFR' THEN 'CHF'
        ELSE COALESCE(NULLIF(UPPER(TRIM(waehrung)), ''), NULL)
    END AS "CurrencyIsoCode",

    -- AccountId: transform source kd_nr (e.g. CUST-1001) → Salesforce Id format (CUS-1001)
    'CUS-' || REGEXP_REPLACE(TRIM(kd_nr), '^CUST-*', '', 'i') AS "AccountId",

    -- Legacy_Opportunity_ID__c: passthrough of source natural key
    TRIM(chance_id) AS "Legacy_Opportunity_ID__c",

    -- CreatedDate / LastModifiedDate: CURRENT_TIMESTAMP placeholder
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",

    -- IsDeleted: default 0
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}