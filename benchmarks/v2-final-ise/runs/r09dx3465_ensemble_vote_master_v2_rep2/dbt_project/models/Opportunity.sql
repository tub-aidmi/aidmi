{{ config(materialized='table') }}

WITH opportunity_base AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

account_key_lookup AS (
    SELECT
        kundennummer,
        '001' || kundennummer AS account_id_sfdc
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_enriched AS (
    SELECT
        o.*,
        a.account_id_sfdc
    FROM opportunity_base o
    LEFT JOIN account_key_lookup a
        ON o.kunden_ref = a.kundennummer
),

parsed_dates AS (
    SELECT
        *,
        -- Parse date from DD.MM.YYYY, YYYYMMDD, or MM/DD/YYYY formats
        COALESCE(
            TRY_TO_DATE(zieldatum, 'YYYY-MM-DD'),
            TRY_TO_DATE(zieldatum, 'DD.MM.YYYY'),
            TRY_TO_DATE(REGEXP_REPLACE(zieldatum, '(\d{4})(\d{2})(\d{2})', '\1-\2-\3'), 'YYYY-MM-DD')
        ) AS parsed_close_date
    FROM opportunity_enriched
),

amounts_cleaned AS (
    SELECT
        *,
        -- Strip currency symbols/text, handle European format (e.g. 1.234,56 -> 1234.56)
        CASE
            WHEN auftragswert IS NOT NULL THEN
                CAST(
                    REGEXP_REPLACE(
                        REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE(auftragswert, '[^0-9.,]', '', 'g'), '\\.', ''),
                        ',', '.'),
                    '^[0-9.]+$'
                    )
                AS DOUBLE PRECISION)
            ELSE NULL
        END AS clean_amount
    FROM parsed_dates
),

stage_mapped AS (
    SELECT
        *,
        CASE LOWER(TRIM(vertriebsphase))
            WHEN 'akquise' THEN 'Prospecting'
            WHEN 'qualifikation' THEN 'Qualification'
            WHEN 'bedarfsermittlung' THEN 'Needs Analysis'
            WHEN 'wertversprechen' THEN 'Value Proposition'
            WHEN 'entscheidungsträger identifizieren' THEN 'Id. Decision Makers'
            WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
            WHEN 'angebot/preisanfrage' THEN 'Proposal/Price Quote'
            WHEN 'angebot/preis' THEN 'Proposal/Price Quote'
            WHEN 'verhandlung' THEN 'Negotiation/Review'
            WHEN 'preisprüfung' THEN 'Negotiation/Review'
            WHEN 'gewonnen' THEN 'Closed Won'
            WHEN 'auftrag' THEN 'Closed Won'
            WHEN 'closed won' THEN 'Closed Won'
            WHEN 'verloren' THEN 'Closed Lost'
            WHEN 'nicht gewonnen' THEN 'Closed Lost'
            WHEN 'closed lost' THEN 'Closed Lost'
            ELSE vertriebsphase
        END AS mapped_stage
    FROM amounts_cleaned
)

SELECT
    -- Id: Salesforce-style Opportunity ID
    '006' || opp_kennung AS "Id",
    -- Name
    COALESCE(TRIM(titel), '') AS "Name",
    -- StageName
    CASE
        WHEN mapped_stage IN (
            'Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition',
            'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote',
            'Negotiation/Review', 'Closed Won', 'Closed Lost'
        ) THEN mapped_stage
        ELSE NULL
    END AS "StageName",
    -- CloseDate: ISO YYYY-MM-DD
    TO_CHAR(parsed_close_date, 'YYYY-MM-DD') AS "CloseDate",
    -- Amount
    clean_amount AS "Amount",
    -- CurrencyIsoCode
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    -- AccountId: reference to Salesforce Account Id (not raw source key)
    account_id_sfdc AS "AccountId",
    -- Legacy_Opportunity_ID__c
    opp_kennung AS "Legacy_Opportunity_ID__c",
    -- CreatedDate/LastModifiedDate: not available in source, set to default
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
FROM stage_mapped;
