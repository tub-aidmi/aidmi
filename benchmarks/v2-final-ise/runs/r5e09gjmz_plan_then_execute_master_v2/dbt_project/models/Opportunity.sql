{{ config(materialized='table') }}

SELECT
    TRIM(mo.opp_kennung) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(mo.titel)), ''), 'Untitled') AS "Name",
    CASE
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('akquise', 'prospect', 'prospecting', 'in kontakt', 'neu', 'new') THEN 'Prospecting'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('qualifikation', 'qualification', 'quali', 'needs analysis', 'analyse', 'bedarfsermittlung') THEN 'Qualification'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('bedarfsermittlung', 'needs analysis', 'analyse', 'in prüfung') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('wertproposition', 'value proposition', 'nutzen', 'solution') THEN 'Value Proposition'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('entscheideridentifikation', 'id. decision makers', 'entscheider', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('angebot', 'proposal/price quote', 'vorschlag', 'kalkulation', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('verhandlung', 'negotiation/review', 'diskussion', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('abgeschlossen gewonnen', 'closed won', 'gewonnen', 'won', 'erfolgreich', 'successfully closed') THEN 'Closed Won'
        WHEN LOWER(TRIM(mo.vertriebsphase)) IN ('abgeschlossen verloren', 'closed lost', 'verloren', 'lost', 'storniert', 'cancelled') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(mo.zieldatum) IS NULL OR TRIM(mo.zieldatum) = '' THEN NULL
        -- ISO format: YYYY-MM-DD
        WHEN TRIM(mo.zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(mo.zieldatum)
        -- DD.MM.YYYY (European)
        WHEN TRIM(mo.zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'DD.MM.YYYY')::TEXT
        -- MM/DD/YYYY (US)
        WHEN TRIM(mo.zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(mo.zieldatum), 'MM/DD/YYYY')::TEXT
        -- YYYYMMDD
        WHEN TRIM(mo.zieldatum) ~ '^\d{8}$' THEN
            SUBSTR(TRIM(mo.zieldatum), 1, 4) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 5, 2) || '-' ||
            SUBSTR(TRIM(mo.zieldatum), 7, 2)
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(mo.auftragswert) IS NULL OR TRIM(mo.auftragswert) = '' OR UPPER(TRIM(mo.auftragswert)) = 'NONE' THEN NULL
        ELSE
            CASE
                -- European format: digits with dot as thousand-sep and comma as decimal (e.g., 1.234,56)
                WHEN REGEXP_REPLACE(TRIM(mo.auftragswert), '[^\d.,\-]', '') ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(mo.auftragswert, '[€£$¤]', '', 'g'),
                            '^\s*(?:EUR|Euro)\s*', '', 'gi'),
                        '\.', '')::DOUBLE PRECISION / NULLIF(NULLIF(REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(mo.auftragswert, '[€£$¤]', '', 'g'),
                                '^\s*(?:EUR|Euro)\s*', '', 'gi'),
                            ',', '.', 1), 0)::DOUBLE PRECISION * NULLIF(NULLIF(REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(mo.auftragswert, '[€£$¤]', '', 'g'),
                                    '^\s*(?:EUR|Euro)\s*', '', 'gi'),
                                ',', '.', 1), 0)::DOUBLE PRECISION
                -- Fallback: simple numeric with optional commas as thousands or dot decimal
                ELSE REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(mo.auftragswert, '[€£$¤]', '', 'g'),
                        '^\s*(?:EUR|Euro)\s*', '', 'gi'),
                    ',', '')::DOUBLE PRECISION
            END
    END AS "Amount",
    CASE UPPER(TRIM(mo.waehrungscode))
        WHEN 'USD' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'DOLLAR' THEN 'USD'
        WHEN 'EUR' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'EURO' THEN 'EUR'
        WHEN 'GBP' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        WHEN 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    'CUS-' || SPLIT_PART(TRIM(mo.kunden_ref), '-', 2) AS "AccountId",
    TRIM(mo.opp_kennung) AS "Legacy_Opportunity_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON SPLIT_PART(TRIM(mo.kunden_ref), '-', 2) = SPLIT_PART(TRIM(mk.kundennummer), '-', 2)