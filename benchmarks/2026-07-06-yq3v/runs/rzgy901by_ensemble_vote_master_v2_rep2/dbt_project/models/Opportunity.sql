-- models/Opportunity.sql

{{ config(materialized='table') }}

WITH opportunity_cleaned_data AS (
    SELECT
        opp.opp_kennung,
        opp.titel,
        opp.vertriebsphase,
        opp.zieldatum,
        opp.waehrungscode,
        opp.kunden_ref,
        TRIM(opp.auftragswert) AS raw_auftragswert,
        REGEXP_REPLACE(TRIM(opp.auftragswert), '[^0-9,.]+', '', 'g') AS cleaned_auftragswert
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opp
)
SELECT
    TRIM(ocd.opp_kennung) AS "Id",
    COALESCE(TRIM(ocd.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('prospecting', 'neu') THEN 'Prospecting'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('qualification', 'qualifizierung') THEN 'Qualification'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('id. decision makers', 'entscheidungsträger identifiziert') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('proposal/price quote', 'angebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('closed won', 'gewonnen', 'abgeschlossen gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(ocd.vertriebsphase)) IN ('closed lost', 'verloren', 'abgeschlossen verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            CASE WHEN TRIM(ocd.zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(ocd.zieldatum), 'YYYY-MM-DD') END,
            CASE WHEN TRIM(ocd.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(ocd.zieldatum), 'DD.MM.YYYY') END,
            CASE WHEN TRIM(ocd.zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(ocd.zieldatum), 'MM/DD/YYYY') END,
            CURRENT_DATE -- Fallback for NOT NULL target column
        ), 'YYYY-MM-DD'
    ) AS "CloseDate",
    NULLIF(
        CAST(
            CASE
                WHEN ocd.cleaned_auftragswert IS NULL OR ocd.cleaned_auftragswert = '' THEN NULL
                -- Case 1: Both comma and dot. Determine locale based on relative position.
                WHEN POSITION(',' IN ocd.cleaned_auftragswert) > 0 AND POSITION('.' IN ocd.cleaned_auftragswert) > 0 THEN
                    CASE
                        WHEN POSITION(',' IN ocd.cleaned_auftragswert) > POSITION('.' IN ocd.cleaned_auftragswert) THEN
                            -- European format (e.g., 1.234,56): comma is decimal, dot is thousand separator.
                            REPLACE(REPLACE(ocd.cleaned_auftragswert, '.', ''), ',', '.')
                        ELSE
                            -- US format (e.g., 1,234.56): dot is decimal, comma is thousand separator.
                            REPLACE(ocd.cleaned_auftragswert, ',', '')
                    END
                -- Case 2: Only comma present. Assume European decimal separator.
                WHEN POSITION(',' IN ocd.cleaned_auftragswert) > 0 THEN
                    REPLACE(ocd.cleaned_auftragswert, ',', '.')
                -- Case 3: Only dot(s) present. Assume the LAST dot is the decimal separator, and all preceding dots are thousand separators.
                WHEN POSITION('.' IN ocd.cleaned_auftragswert) > 0 THEN
                    REGEXP_REPLACE(ocd.cleaned_auftragswert, '\.(?![^.]*$)', '', 'g')
                ELSE
                    -- No comma or dot, just digits.
                    ocd.cleaned_auftragswert
            END AS TEXT
        ), ''
    )::DOUBLE PRECISION AS "Amount",
    TRIM(ocd.waehrungscode) AS "CurrencyIsoCode",
    TRIM(ocd.kunden_ref) AS "AccountId",
    TRIM(ocd.opp_kennung) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    opportunity_cleaned_data AS ocd