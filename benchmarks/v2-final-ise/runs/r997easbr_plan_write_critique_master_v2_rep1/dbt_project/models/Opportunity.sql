{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
cleaned_amounts AS (
    SELECT
        *,
        CASE
            -- Already clean digits, optional dot or comma decimal
            WHEN TRIM(auftragswert) ~ '^\-?\d+(\.\d{1,2})?$' THEN CAST(TRIM(auftragswert) AS DOUBLE PRECISION)
            -- European format: 1.234,56 (dots = thousands, comma = decimal)
            WHEN TRIM(auftragswert) ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
                CAST(
                    REGEXP_REPLACE(REGEXP_REPLACE(TRIM(auftragswert), '[^0-9.,\-]', '', 'g'), '[ ]', '', 'g')
                    -- Remove dots (thousand sep), swap comma to dot
                    || '' AS DOUBLE PRECISION)
            WHEN TRIM(auftragswert) ~ '^\-?\d+,\d+$' THEN NULL  -- ambiguous: could be European decimal or error
            ELSE NULL
        END as clean_amount
    FROM source
)

SELECT
    -- Id: Salesforce-style ID with '006' prefix
    '006' || UPPER(TRIM(opp_kennung)) AS "Id",

    -- Name: Title with INITCAP, fallback for NULL/empty
    COALESCE(NULLIF(INITCAP(TRIM(titel)), ''), 'Unknown Opportunity') AS "Name",

    -- StageName: Map German and English values to Salesforce enum
    CASE
        WHEN LOWER(TRIM(vertriebsphase)) IN ('prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('bedürfnisanalyse', 'needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('wert proposition', 'value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('entscheider identifizieren', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('wahrnehmungsanalyse', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('angebot preis', 'proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('verhandlung review', 'negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('abgeschlossen (gewonnen)', 'closed won', 'won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(vertriebsphase)) IN ('abgeschlossen (verloren)', 'closed lost', 'lost', 'verloren') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: Multi-format date parser, output ISO YYYY-MM-DD
    CASE
        WHEN TRIM(zieldatum) IS NULL OR TRIM(zieldatum) = '' THEN NULL
         -- DD.MM.YYYY format (German style)
        WHEN TRIM(zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
         -- YYYY-MM-DD format (ISO style)
        WHEN TRIM(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(zieldatum)
         -- MM/DD/YYYY format (US style, e.g., "1/28/2025" or "12/31/2025")
        WHEN TRIM(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(zieldatum), 'MM/DD/YYYY')::TEXT
         -- YYYYMMDD format (8 digits, e.g., "20260201")
        WHEN TRIM(zieldatum) ~ '^\d{8}$' THEN
            TO_DATE(
                SUBSTR(TRIM(zieldatum), 1, 4) || '-' ||
                SUBSTR(TRIM(zieldatum), 5, 2) || '-' ||
                SUBSTR(TRIM(zieldatum), 7, 2),
                'YYYY-MM-DD'
            )::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: Clean European and other number formats
    CASE
        WHEN TRIM(auftragswert) IS NULL OR LOWER(TRIM(auftragswert)) IN ('none', 'null', 'n/a') THEN NULL
        -- European format with dots as thousands and comma as decimal: e.g. "400.902,63" -> 400902.63
        WHEN TRIM(auftragswert) ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' THEN
            CAST(
                REPLACE(REPLACE(TRIM(auftragswert), '.', ''), ',', '.') AS DOUBLE PRECISION
            )
        -- Clean numeric with comma decimal but no thousands (e.g. "1234,56") - rare in this dataset
        WHEN TRIM(auftragswert) ~ '^\-?\d+,\d+$' AND LENGTH(TRIM(auftragswert)) < 8 THEN
            CAST(REPLACE(TRIM(auftragswert), ',', '.') AS DOUBLE PRECISION)
        -- Clean numeric with dot decimal (e.g. "10110.16") or just digits
        WHEN TRIM(auftragswert) ~ '^\-?\d+(\.\d+)?$' THEN CAST(TRIM(auftragswert) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",

    -- CurrencyIsoCode: Normalize various representations to ISO 4217 codes
    CASE
        WHEN TRIM(waehrungscode) = '$' OR UPPER(TRIM(waehrungscode)) IN ('USD', 'DOLLAR') THEN 'USD'
        WHEN TRIM(waehrungscode) = '€' OR UPPER(TRIM(waehrungscode)) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN TRIM(waehrungscode) = '£' OR UPPER(TRIM(waehrungscode)) IN ('GBP', 'POUND') THEN 'GBP'
        WHEN LOWER(TRIM(waehrungscode)) = 'chf' THEN 'CHF'
        ELSE COALESCE(UPPER(TRIM(waehrungscode)), NULL)
    END AS "CurrencyIsoCode",

    -- AccountId: Transform KD- prefix to CUST-, then apply A00 Salesforce prefix
    -- Source key: "KD-M1165" -> Remove "KD-" -> "M1165" -> Apply "A00" prefix -> "A00CUST-M1165"
    'A00' || UPPER('CUST-' || TRIM(SUBSTR(TRIM(kunden_ref), 4))) AS "AccountId",

    -- Legacy_Opportunity_ID__c: Preserve raw source key for reconciliation
    TRIM(opp_kennung) AS "Legacy_Opportunity_ID__c",

    -- CreatedDate / LastModifiedDate: Not in source, use static default
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",

    -- IsDeleted: 0 = not deleted
    0 AS "IsDeleted"

FROM source