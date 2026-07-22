
-- depends_on: {{ source('fixture_master_src', 'master_opportunities') }}

{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        waehrungscode,
        kunden_ref,
        -- Clean up auftragswert by removing currency symbols/text
        CASE
            WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
            ELSE
                TRIM(
                    REGEXP_REPLACE(
                        LOWER(auftragswert),
                        'eur |usd |chf |gbp |€ |dollar', '', 'g'
                    )
                )
        END AS cleaned_auftragswert_prep
    FROM
        {{ source('fixture_master_src', 'master_opportunities') }}
)
SELECT
    co.opp_kennung AS "Id",
    COALESCE(TRIM(co.titel), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(co.vertriebsphase) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen', 'closedwon') THEN 'Closed Won'
        WHEN LOWER(co.vertriebsphase) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(co.vertriebsphase) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(co.vertriebsphase) IN ('qualifikation', 'quali', 'qualification', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(co.vertriebsphase) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(co.vertriebsphase) IN ('value proposition', 'wertangebot') THEN 'Value Proposition'
        WHEN LOWER(co.vertriebsphase) IN ('id. decision makers', 'entscheidungsträger identifizieren') THEN 'Id. Decision Makers'
        WHEN LOWER(co.vertriebsphase) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(co.vertriebsphase) IN ('proposal/price quote', 'angebot/preisangebot') THEN 'Proposal/Price Quote'
        WHEN LOWER(co.vertriebsphase) IN ('negotiation/review', 'verhandlung/überprüfung') THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL StageName
    END AS "StageName",
    COALESCE(
        CASE
            WHEN co.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN co.zieldatum -- YYYY-MM-DD
            WHEN co.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            WHEN co.zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN co.zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(co.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            ELSE NULL
        END,
        '1900-01-01' -- Default for NOT NULL CloseDate
    ) AS "CloseDate",
    CAST(
        CASE
            WHEN co.cleaned_auftragswert_prep IS NULL OR TRIM(co.cleaned_auftragswert_prep) = '' THEN NULL
            WHEN co.cleaned_auftragswert_prep ~ '^[0-9\.,]+$' THEN
                CASE
                    -- European format: thousands separator is '.' and decimal is ','
                    WHEN POSITION('''' IN co.cleaned_auftragswert_prep) > 0
                        AND POSITION('.' IN co.cleaned_auftragswert_prep) > 0
                        AND POSITION('''' IN co.cleaned_auftragswert_prep) > POSITION('.' IN co.cleaned_auftragswert_prep)
                    THEN
                        REPLACE(REPLACE(co.cleaned_auftragswert_prep, '.', ''), '''', '.')
                    -- Only comma as decimal separator
                    WHEN POSITION('''' IN co.cleaned_auftragswert_prep) > 0
                    THEN
                        REPLACE(co.cleaned_auftragswert_prep, '''', '.')
                    -- Assume standard format (dot as decimal, or integer)
                    ELSE
                        co.cleaned_auftragswert_prep
                END
            ELSE NULL
        END
    AS DOUBLE PRECISION) AS "Amount",
    COALESCE(
        CASE
            WHEN LOWER(co.waehrungscode) = 'eur' THEN 'EUR'
            WHEN LOWER(co.waehrungscode) = 'usd' THEN 'USD'
            WHEN LOWER(co.waehrungscode) = 'chf' THEN 'CHF'
            WHEN LOWER(co.waehrungscode) = 'gbp' THEN 'GBP'
            WHEN LOWER(co.waehrungscode) = '€' THEN 'EUR'
            WHEN LOWER(co.waehrungscode) = 'dollar' THEN 'USD'
            ELSE NULL
        END, 'USD' -- Default currency if source is unparseable
    ) AS "CurrencyIsoCode",
    REPLACE(co.kunden_ref, 'KD-', 'CUST-') AS "AccountId",
    co.opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS co
