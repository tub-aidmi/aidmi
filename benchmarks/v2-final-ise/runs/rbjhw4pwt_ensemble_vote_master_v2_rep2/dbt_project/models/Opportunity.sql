{{ config(materialized='table') }}

WITH account_key_lookup AS (
    SELECT
        kundennummer,
        '001' || kundennummer AS account_id_sfdc
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_raw AS (
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

opportunity_enriched AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        o.waehrungscode,
        o.kunden_ref,
        a.account_id_sfdc
    FROM opportunity_raw o
    LEFT JOIN account_key_lookup a
        ON o.kunden_ref = a.kundennummer
),

with_parsed_date AS (
    SELECT
        *,
        CASE
            WHEN TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
            ELSE NULL
        END AS parsed_close_date
    FROM opportunity_enriched
),

with_clean_amount AS (
    SELECT
        *,
        CASE
            WHEN TRIM(auftragswert) IS NOT NULL AND TRIM(auftragswert) != ''
            THEN CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(auftragswert), '[^0-9.,]', '', 'g'),
                    '\.', ''),
                ',', '.') AS DOUBLE PRECISION)
            ELSE NULL
        END AS clean_amount
    FROM with_parsed_date
),

with_stage AS (
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
            ELSE NULL
        END AS mapped_stage
    FROM with_clean_amount
)

SELECT
    '006' || opp_kennung AS "Id",
    COALESCE(TRIM(titel), '') AS "Name",
    COALESCE(mapped_stage, 'Prospecting') AS "StageName",
    COALESCE(parsed_close_date, '1900-01-01') AS "CloseDate",
    clean_amount AS "Amount",
    TRIM(waehrungscode) AS "CurrencyIsoCode",
    account_id_sfdc AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM with_stage;