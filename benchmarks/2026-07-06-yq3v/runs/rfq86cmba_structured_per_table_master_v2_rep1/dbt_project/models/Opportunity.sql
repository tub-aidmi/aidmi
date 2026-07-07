-- depends_on: {{ source('fixture_master_v2_src', 'master_opportunities') }}
-- depends_on: {{ source('fixture_master_v2_src', 'master_kunden') }}

{{ config(materialized='table') }}

WITH opportunities_stg AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        -- Clean and normalize amount string by removing any characters that are not digits, comma, or dot
        CASE
            WHEN TRIM(auftragswert) IS NULL OR TRIM(auftragswert) = '' THEN NULL
            ELSE
                REGEXP_REPLACE(
                    TRIM(auftragswert),
                    '[^0-9,.]', '', 'g'
                )
        END AS cleaned_auftragswert,
        waehrungscode,
        kunden_ref
    FROM
        opportunities_stg
)
SELECT
    opp.opp_kennung AS "Id",
    COALESCE(TRIM(opp.titel), opp.opp_kennung) AS "Name",
    CASE
        WHEN opp.vertriebsphase ILIKE 'Initialer Kontakt' THEN 'Prospecting'
        WHEN opp.vertriebsphase ILIKE 'Qualifizierung' THEN 'Qualification'
        WHEN opp.vertriebsphase ILIKE 'Bedarfsanalyse' THEN 'Needs Analysis'
        WHEN opp.vertriebsphase ILIKE 'Wertversprechen' THEN 'Value Proposition'
        WHEN opp.vertriebsphase ILIKE 'Entscheidungsträger identifiziert' THEN 'Id. Decision Makers'
        WHEN opp.vertriebsphase ILIKE 'Angebots-/Preisangebot' THEN 'Proposal/Price Quote'
        WHEN opp.vertriebsphase ILIKE 'Verhandlung/Überprüfung' THEN 'Negotiation/Review'
        WHEN opp.vertriebsphase ILIKE 'Gewonnen' THEN 'Closed Won'
        WHEN opp.vertriebsphase ILIKE 'Verloren' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting for unknown stages
    END AS "StageName",
    CASE
        WHEN opp.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default to current date if no known format matches or parsing fails
    END AS "CloseDate",
    CASE
        WHEN opp.cleaned_auftragswert IS NULL THEN NULL
        WHEN opp.cleaned_auftragswert ~ '^[0-9]+(\.[0-9]{3})*,[0-9]+$' -- European format (e.g., 1.234,56)
            THEN CAST(REPLACE(REPLACE(opp.cleaned_auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN opp.cleaned_auftragswert ~ '^[0-9]+(,[0-9]{3})*\.[0-9]+$' -- US format (e.g., 1,234.56)
            THEN CAST(REPLACE(opp.cleaned_auftragswert, ',', '') AS DOUBLE PRECISION)
        WHEN opp.cleaned_auftragswert ~ '^[0-9]+$' -- Integer only
            THEN CAST(opp.cleaned_auftragswert AS DOUBLE PRECISION)
        WHEN opp.cleaned_auftragswert ~ '^[0-9]*\.[0-9]+$' -- Decimal with dot (e.g., 123.45)
            THEN CAST(opp.cleaned_auftragswert AS DOUBLE PRECISION)
        WHEN opp.cleaned_auftragswert ~ '^[0-9]*,[0-9]+$' -- Decimal with comma (assume European decimal, no thousands sep)
            THEN CAST(REPLACE(opp.cleaned_auftragswert, ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    opp.waehrungscode AS "CurrencyIsoCode",
    md5(cust.kundennummer) AS "AccountId",
    opp.opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities AS opp
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS cust
    ON opp.kunden_ref = cust.kundennummer