{{ config(materialized='table') }}

WITH raw_opps AS (
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

cleaned_amounts AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        CASE
            WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
            ELSE REGEXP_REPLACE(
                TRIM(REGEXP_REPLACE(TRIM(auftragswert), '^\s*(?:EUR\s*|€\s*)', '', 'gi')),
                 '[^0-9.,\-]', ''
             )
        END AS raw_cleaned_amount
    FROM raw_opps
),

amount_parsed AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        CASE
            WHEN raw_cleaned_amount IS NOT NULL AND raw_cleaned_amount ~ '^\-?\d+$'
                THEN CAST(raw_cleaned_amount AS DOUBLE PRECISION)

            WHEN raw_cleaned_amount IS NOT NULL AND raw_cleaned_amount ~ '^\-?\d+\.\d+$'
                THEN CAST(raw_cleaned_amount AS DOUBLE PRECISION)

            WHEN raw_cleaned_amount IS NOT NULL AND raw_cleaned_amount ~ '^\-?\d+,\d+$'
                THEN CAST(REPLACE(raw_cleaned_amount, ',', '.') AS DOUBLE PRECISION)

            WHEN raw_cleaned_amount IS NOT NULL
                 AND POSITION(',' IN raw_cleaned_amount) > 0
                 AND POSITION('.' IN raw_cleaned_amount) > 0
            THEN
                CAST(REPLACE(REPLACE(raw_cleaned_amount, '.', ''), ',', '.') AS DOUBLE PRECISION)

            ELSE NULL
        END AS amount_final
    FROM cleaned_amounts
),

opp_stage_mapped AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        CASE
            WHEN UPPER(TRIM(vertriebsphase)) = 'PROSPEKTING' THEN 'Prospecting'
            WHEN UPPER(TRIM(vertriebsphase)) = 'QUALIFIZIERUNG' THEN 'Qualification'
            WHEN UPPER(TRIM(vertriebsphase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
            WHEN UPPER(TRIM(vertriebsphase)) = 'WERTPROPOSITION' THEN 'Value Proposition'
            WHEN UPPER(TRIM(vertriebsphase)) = 'ENTSCHEIDUNGSFINDER' THEN 'Id. Decision Makers'
            WHEN UPPER(TRIM(vertriebsphase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
            WHEN UPPER(TRIM(vertriebsphase)) = 'ANGEBOT' THEN 'Proposal/Price Quote'
            WHEN UPPER(TRIM(vertriebsphase)) = 'VERHANDLUNG' THEN 'Negotiation/Review'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('ABGESCHLOSSEN_GEWINN', 'GEWONNEN') THEN 'Closed Won'
            WHEN UPPER(TRIM(vertriebsphase)) IN ('ABGESCHLOSSEN_VERLOREN', 'VERLOREN') THEN 'Closed Lost'
            ELSE NULL
        END AS stage_name_clean,
        CASE
            WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_DATE(TRIM(zieldatum), 'DD.MM.YYYY')::TEXT
            WHEN zieldatum IS NOT NULL AND TRIM(zieldatum) ~ '^\d{8}$'
                THEN SUBSTRING(TRIM(zieldatum), 1, 4) || '-' || SUBSTRING(TRIM(zieldatum), 5, 2) || '-' || SUBSTRING(TRIM(zieldatum), 7, 2)
            ELSE NULL
        END AS close_date_iso

    FROM amount_parsed
),

opp_with_account AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.stage_name_clean,
        o.close_date_iso,
        o.amount_final,
        o.waehrungscode,
        c.kundennummer AS account_id_ref
    FROM opp_stage_mapped o
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
        ON LOWER(TRIM(o.kunden_ref)) = LOWER(TRIM(c.kundennummer))
)

SELECT
    opp_kennung AS "Id",
    COALESCE(NULLIF(TRIM(titel), ''), 'Unnamed Opportunity') AS "Name",
    stage_name_clean AS "StageName",
    close_date_iso AS "CloseDate",
    amount_final AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    account_id_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM opp_with_account