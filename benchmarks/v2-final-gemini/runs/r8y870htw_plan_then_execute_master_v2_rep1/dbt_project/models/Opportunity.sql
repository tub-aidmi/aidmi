{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        o.opp_kennung,
        o.titel,
        o.vertriebsphase,
        o.zieldatum,
        o.auftragswert,
        o.waehrungscode,
        o.kunden_ref,
        k.kundennummer AS kunden_kundennummer,
        -- Clean auftragswert
        NULLIF(
            CASE
                WHEN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g') ~ '^'[0-9]+\.[0-9]+,[0-9]+$' THEN -- European format like 1.234,56
                    REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g'), '\.', '', 'g'), ',', '.')
                WHEN REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g') ~ '^'[0-9]+,[0-9]+$' THEN -- European format like 1234,56
                    REPLACE(REGEXP_REPLACE(o.auftragswert, '[^0-9,.]', '', 'g'), ',', '.')
                ELSE -- US format or just numbers
                    REGEXP_REPLACE(o.auftragswert, '[^0-9.]', '', 'g')
            END,
            ''
        ) AS cleaned_amount_str
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON
        o.kunden_ref = k.kundennummer
)

SELECT
    MD5(o.opp_kennung) AS "Id",
    COALESCE(o.titel, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN o.vertriebsphase = 'Anbahnung' THEN 'Prospecting'
        WHEN o.vertriebsphase = 'Qualifizierung' THEN 'Qualification'
        WHEN o.vertriebsphase = 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN o.vertriebsphase = 'Wertangebot' THEN 'Value Proposition'
        WHEN o.vertriebsphase = 'Entscheider Identifikation' THEN 'Id. Decision Makers'
        WHEN o.vertriebsphase = 'Wahrnehmungsanalyse' THEN 'Perception Analysis'
        WHEN o.vertriebsphase = 'Angebot/Preisangebot' THEN 'Proposal/Price Quote'
        WHEN o.vertriebsphase = 'Verhandlung/Prüfung' THEN 'Negotiation/Review'
        WHEN o.vertriebsphase = 'Geschlossen gewonnen' THEN 'Closed Won'
        WHEN o.vertriebsphase = 'Geschlossen verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting for NOT NULL constraint
    END AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        '2023-01-01' -- Default value for NOT NULL
    ) AS "CloseDate",
    CAST(o.cleaned_amount_str AS DOUBLE PRECISION) AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    MD5(o.kunden_kundennummer) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS o