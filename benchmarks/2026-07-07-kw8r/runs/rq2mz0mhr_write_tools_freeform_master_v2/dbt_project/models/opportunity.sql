{{ config(materialized='table') }}

WITH opp_clean AS (
    SELECT
        o.opp_kennung,
        INITCAP(TRIM(o.titel)) AS titel,
        UPPER(TRIM(o.vertriebsphase)) AS vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        UPPER(TRIM(o.waehrungscode)) AS waehrungscode_raw,
        TRIM(o.kunden_ref) AS kunden_ref_raw
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
),
parsed_date AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode_raw,
        kunden_ref_raw,
        CASE
            WHEN zieldatum IS NULL OR TRIM(zieldatum) = '' THEN NULL
            WHEN zieldatum ~ '^\d{8}$' THEN TO_DATE(zieldatum, 'YYYYMMDD')::TEXT
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')::TEXT
            ELSE NULL
        END AS zieldatum_parsed,
        CASE
            WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
            WHEN UPPER(TRIM(auftragswert)) IN ('NONE', 'N/A', 'NULL', '-') THEN NULL
            WHEN UPPER(auftragswert) ~ '^\s*[A-Za-z]+\s' THEN REGEXP_REPLACE(auftragswert, '[^0-9.,\-]', '', 'g')
            ELSE auftragswert
        END AS auftragswert_clean
    FROM opp_clean
),
amount_parsed AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum_parsed,
        waehrungscode_raw,
        kunden_ref_raw,
        CASE
            WHEN TRIM(auftragswert_clean) = '' OR auftragswert_clean IS NULL THEN NULL
            WHEN UPPER(TRIM(auftragswert_clean)) IN ('NONE', 'N/A', 'NULL', '-') THEN NULL
             -- European format: dot before comma (e.g., "400.902,63")
            WHEN auftragswert_clean ~ '\.' AND auftragswert_clean ~ ',' AND POSITION('.' IN auftragswert_clean) < POSITION(',' IN auftragswert_clean) THEN
                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert_clean), '\.', '', 'g'), ',', '.', 'g')::DOUBLE PRECISION
             -- Comma-only decimal (e.g., "1234,56")
            WHEN auftragswert_clean ~ ',' AND NOT (auftragswert_clean ~ '\.') THEN
                REGEXP_REPLACE(TRIM(auftragswert_clean), ',', '.', 'g')::DOUBLE PRECISION
             -- Pure numeric or US format (e.g., "1234.56")
            ELSE TRIM(auftragswert_clean)::DOUBLE PRECISION
        END AS amount_val
    FROM parsed_date
)
SELECT
    CAST(UPPER(TRIM(opp_kennung)) AS TEXT) AS "Id",
    titel AS "Name",
    CASE
        WHEN vertriebsphase IN ('PROSPECTING', 'PROSPEKTING') THEN 'Prospecting'
        WHEN vertriebsphase IN ('QUALIFICATION', 'QUALIFIZIERUNG') THEN 'Qualification'
        WHEN vertriebsphase IN ('NEEDS ANALYSIS', 'BEDARFSANALYSE') THEN 'Needs Analysis'
        WHEN vertriebsphase IN ('VALUE PROPOSITION', 'WERTVORSCHLAG', 'VALUE_PROPOSITION') THEN 'Value Proposition'
        WHEN vertriebsphase IN ('ID DECISION MAKERS', 'ENTSCHEIDER IDENTIFIZIEREN', 'ID_DECISION_MAKERS') THEN 'Id. Decision Makers'
        WHEN vertriebsphase IN ('PERCEPTION ANALYSIS', 'WAHRNEHMUNGSANALYSE') THEN 'Perception Analysis'
        WHEN vertriebsphase IN ('PROPOSAL/PRICE QUOTE', 'ANGEBOT', 'PROPOSAL_PRICE_QUOTE', 'PROPOSAL / PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN vertriebsphase IN ('NEGOTIATION/REVIEW', 'VERHANDLUNG', 'NEGOTIATION_REVIEW', 'NEGOTIATION / REVIEW') THEN 'Negotiation/Review'
        WHEN vertriebsphase IN ('CLOSED WON', 'ABGESCHLOSSEN GEWINNT', 'CLOSED_WON', 'WON') THEN 'Closed Won'
        WHEN vertriebsphase IN ('CLOSED LOST', 'VERLOREN', 'CLOSED_LOST', 'LOST') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    zieldatum_parsed AS "CloseDate",
    amount_val AS "Amount",
    CASE
        WHEN waehrungscode_raw = 'EUR' THEN 'EUR'
        WHEN waehrungscode_raw IN ('USD', 'US-DOLLAR') THEN 'USD'
        WHEN waehrungscode_raw IN ('GBP', 'POUND') THEN 'GBP'
        WHEN waehrungscode_raw IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    CAST(UPPER(TRIM(kunden_ref_raw)) AS TEXT) AS "AccountId",
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",
     '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM amount_parsed
