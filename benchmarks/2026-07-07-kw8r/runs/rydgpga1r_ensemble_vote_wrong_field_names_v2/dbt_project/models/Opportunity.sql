{{ config(materialized='table') }}

SELECT
    -- Salesforce-style ID derived from legacy opportunity key
    CAST('006' || LPAD(chance_id, 9, '0') AS TEXT) AS "Id",
    
    -- Opportunity name from source bezeichnung
    INITCAP(TRIM(bezeichnung)) AS "Name",
    
    -- StageName mapping: German phase values to Salesforce enum format
    CASE 
        WHEN LOWER(TRIM(phase)) LIKE '%neu%' OR LOWER(TRIM(phase)) LIKE '%prospekt%' THEN 'Prospecting'
        WHEN LOWER(TRIM(phase)) LIKE '%qualif%' THEN 'Qualification'
        WHEN LOWER(TRIM(phase)) LIKE '%bedürfn%' OR LOWER(TRIM(phase)) LIKE '%need%' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) LIKE '%wert%' OR LOWER(TRIM(phase)) LIKE '%value%' OR LOWER(TRIM(phase)) LIKE '%vorschlag%' THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) LIKE '%entscheid%' OR LOWER(TRIM(phase)) LIKE '%decision%' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) LIKE '%wahrnehm%' OR LOWER(TRIM(phase)) LIKE '%percept%' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) LIKE '%angebot%' OR LOWER(TRIM(phase)) LIKE '%proposal%' OR LOWER(TRIM(phase)) LIKE '%price%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) LIKE '%verhand%' OR LOWER(TRIM(phase)) LIKE '%negotiat%' OR LOWER(TRIM(phase)) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) LIKE '%abgeschloss%' OR LOWER(TRIM(phase)) LIKE '%gewon%' OR LOWER(TRIM(phase)) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) LIKE '%verloren%' OR LOWER(TRIM(phase)) LIKE '%lost%' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    
    -- CloseDate: parse DD.MM.YYYY or YYYY-MM-DD format to ISO format
    CASE 
        WHEN TRIM(abschlussdatum) IS NULL OR TRIM(abschlussdatum) = '' THEN NULL
        WHEN TRIM(abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN LEFT(TRIM(abschlussdatum), 10)
        ELSE NULL
    END AS "CloseDate",
    
    -- Amount: direct from volumne (double precision)
    CAST(volumen AS DOUBLE PRECISION) AS "Amount",
    
    -- CurrencyIsoCode: strip any prefix/suffix and normalize to ISO code
    CASE 
        WHEN TRIM(waehrung) IS NULL OR TRIM(waehrung) = '' THEN NULL
        ELSE UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(waehrung, '[^A-Z]', '', 'g'), '^([A-Z]{3}).*$', '\\1')))
    END AS "CurrencyIsoCode",
    
    -- AccountId: transform source kd_nr to Salesforce-style Account ID format
    CASE 
        WHEN TRIM(kd_nr) IS NULL OR TRIM(kd_nr) = '' THEN NULL
        ELSE CAST('001' || LPAD(TRIM(kd_nr), 9, '0') AS TEXT)
    END AS "AccountId",
    
    -- Legacy_Opportunity_ID__c: preserve source key for traceability
    CAST(chance_id AS TEXT) AS "Legacy_Opportunity_ID__c",
    
    -- Timestamps (source has none - use placeholder defaults)
    '1970-01-01 00:00:00' AS "CreatedDate",
    '1970-01-01 00:00:00' AS "LastModifiedDate",
    
    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}