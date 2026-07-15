{{ config(materialized='table') }}

WITH account_keys AS (
    SELECT
        SPLIT_PART(TRIM(kundennummer), '-', 2) AS normalized_id,
        kundennummer AS legacy_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
clean_amounts AS (
    SELECT
        mo.*,
        -- Remove any currency prefix text like "EUR", "USD", "Dollar", etc. and symbols
        REGEXP_REPLACE(
            TRIM(mo.auftragswert),
            '^[^0-9\-.]+',  ''
        ) AS cleaned_amount_raw
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} mo
)

SELECT
    TRIM(ca.opp_kennung) AS "Id",
    COALESCE(NULLIF(TRIM(ca.titel), ''), 'Untitled') AS "Name",
    CASE
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('akquise', 'prospect', 'prospecting', 'in kontakt', 'neu', 'new') THEN 'Prospecting'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('bedarfsermittlung', 'needs analysis', 'analyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('in prüfung', 'value proposition', 'nutzen', 'solution') THEN 'Value Proposition'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('entscheideridentifikation', 'id. decision makers', 'entscheider', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('angebot', 'proposal/price quote', 'vorschlag', 'kalkulation', 'quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN ('verhandlung', 'negotiation/review', 'diskussion', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN (
             'abgeschlossen gewonnen', 'closed won', 'gewonnen', 'won',
             'abgeschlossen (gewonnen)'
         ) THEN 'Closed Won'
        WHEN LOWER(TRIM(ca.vertriebsphase)) IN (
             'abgeschlossen verloren', 'closed lost', 'verloren', 'lost',
             'abgeschlossen (verloren)'
         ) THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(ca.zieldatum) IS NULL OR TRIM(ca.zieldatum) = '' THEN NULL
        WHEN TRIM(ca.zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(ca.zieldatum)
        WHEN TRIM(ca.zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ca.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(ca.zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ca.zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(ca.zieldatum) ~ '^\d{8}$' THEN
            SUBSTR(TRIM(ca.zieldatum), 1, 4) || '-' ||
            SUBSTR(TRIM(ca.zieldatum), 5, 2) || '-' ||
            SUBSTR(TRIM(ca.zieldatum), 7, 2)
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN ca.cleaned_amount_raw IS NULL OR ca.cleaned_amount_raw = ''
             OR UPPER(TRIM(ca.auftragswert)) IN ('NONE', 'N/A') THEN NULL
        ELSE CAST(
            CASE
                -- European format: has dots as thousand separators and a comma as decimal (e.g. "159.961,05")
                WHEN ca.cleaned_amount_raw ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
                    REGEXP_REPLACE(ca.cleaned_amount_raw, '\.', '') || '.' ||
                    SPLIT_PART(REGEXP_REPLACE(ca.cleaned_amount_raw, '\.', ''), ',', -1)
                -- Already has comma as decimal (e.g. "159,05") — just swap comma to dot
                WHEN ca.cleaned_amount_raw ~ '^\-?\d+,\d+$' THEN
                    REGEXP_REPLACE(ca.cleaned_amount_raw, ',', '.')
                -- US format or plain integer (e.g. "299732.16" or "0")
                WHEN ca.cleaned_amount_raw ~ '^\-?\d+\.\d+$' OR ca.cleaned_amount_raw ~ '^\-?\d+$' THEN
                    ca.cleaned_amount_raw
                -- Fallback: try to extract only numeric chars including minus, dot, comma
                ELSE
                    CASE
                        WHEN ca.cleaned_amount_raw ~ '[,]' THEN
                            REGEXP_REPLACE(ca.cleaned_amount_raw, '[^0-9\-.]', '')
                        ELSE
                            REGEXP_REPLACE(ca.cleaned_amount_raw, '[^\d\-\.]', '')
                    END
            END
        AS DOUBLE PRECISION)
    END AS "Amount",
    CASE UPPER(TRIM(REGEXP_REPLACE(ca.waehrungscode, '\s+', '', 'g')))
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
    ak.normalized_id AS "AccountId",
    TRIM(ca.opp_kennung) AS "Legacy_Opportunity_ID__c",
    NOW()::TEXT AS "CreatedDate",
    NOW()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM clean_amounts ca
LEFT JOIN account_keys ak
    ON SPLIT_PART(TRIM(ca.kunden_ref), '-', 2) = ak.normalized_id