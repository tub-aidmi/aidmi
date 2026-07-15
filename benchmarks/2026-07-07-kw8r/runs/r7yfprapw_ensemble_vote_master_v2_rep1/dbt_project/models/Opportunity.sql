{{ config(materialized='table') }}

WITH opportunity_source AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        -- Map KD-M* to CUST-M* for Account joining
        REPLACE(kunden_ref, 'KD-', 'CUST-') AS mapped_kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
account_mapping AS (
    SELECT
        kundennummer AS customer_id,
        -- Use kundennummer as a proxy for Account Id (Salesforce-style)
        'ACCT-' || REPLACE(kundennummer, 'CUST-', '') AS account_sf_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    -- Generate a Salesforce-style Id for Opportunity
    'OPP_' || REPLACE(o.opp_kennung, 'OPP-', '') AS "Id",
    
    o.titel AS "Name",
    
    -- Map StageName to enum domain
    CASE 
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('closed lost', 'abgeschlossen (verloren)', 'lost', 'verloren') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.vertriebsphase)) IN ('in kontakt', 'contact') THEN 'Prospecting'
        ELSE NULL
    END AS "StageName",
    
    -- Parse CloseDate from multiple formats
    CASE
        WHEN o.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN o.zieldatum
        WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.zieldatum ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.zieldatum IS NULL THEN NULL
        ELSE NULL
    END AS "CloseDate",
    
    -- Parse Amount: handle European format, currency prefixes, and negatives
    CASE
        WHEN o.auftragswert = 'None' THEN NULL
        WHEN o.auftragswert ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN
            -- European format: 400.902,63 -> 400902.63
            CAST(REPLACE(REPLACE(o.auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[0-9]+,[0-9]{2}$' THEN
            -- European format without dots: 123,45 -> 123.45
            CAST(REPLACE(o.auftragswert, ',', '.') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[A-Za-z]+ [0-9]+\.[0-9]+$' THEN
            -- Currency prefix: EUR 123.45 -> 123.45
            CAST(REGEXP_REPLACE(o.auftragswert, '^[A-Za-z€$£]+ ', '') AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[-+]?[0-9]+\.[0-9]+$' THEN
            -- Plain decimal: 123.45 or -123.45
            CAST(o.auftragswert AS DOUBLE PRECISION)
        WHEN o.auftragswert ~ '^[-+]?[0-9]+$' THEN
            -- Plain integer: 123 or -123
            CAST(o.auftragswert AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    
    -- Normalize CurrencyIsoCode
    CASE
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('eur', '€', 'euro') THEN 'EUR'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('chf', 'chf ') THEN 'CHF'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(o.waehrungscode)) IN ('gbp', '£', 'pound') THEN 'GBP'
        ELSE UPPER(TRIM(o.waehrungscode))
    END AS "CurrencyIsoCode",
    
    -- AccountId: join to account_mapping using mapped_kunden_ref
    am.account_sf_id AS "AccountId",
    
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM opportunity_source o
LEFT JOIN account_mapping am ON o.mapped_kunden_ref = am.customer_id